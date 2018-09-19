require "fileutils"

abort("$MWA_DIR not defined.") unless ENV["MWA_DIR"]
abort("$SRCLIST_ROOT not defined.") unless ENV["SRCLIST_ROOT"]
$mwa_dir = ENV["MWA_DIR"].chomp('/')
$project = ENV["PAWSEY_PROJECT"]


class Float
    def close_to?(n, tol: 0.1)
        (self - n).abs < tol
    end
end

class String
    # https://stackoverflow.com/questions/1489183/colorized-ruby-output
    def colorize(color_code)
        "\e[#{color_code}m#{self}\e[0m"
    end

    def red
        colorize(31)
    end

    def green
        colorize(32)
    end

    def yellow
        colorize(33)
    end

    def blue
        colorize(34)
    end

    def pink
        colorize(35)
    end
end

def get_queue(machine:, user:)
    queue = `squeue -M #{machine} -u #{user} 2>&1`
    # Sometimes the queue system needs a little time to think.
    while queue =~ /error/
        STDERR.puts "Slurm error, waiting 10s"
        STDERR.puts "Error: #{queue}"
        sleep 10
        queue = `squeue -M #{machine} -u #{user} 2>&1`
    end

    jobs_in_queue = queue.split("\n").map { |l| l.split[0].to_i if l =~ /^\d/ }.compact
    len_queue = jobs_in_queue.length
    return queue, jobs_in_queue, len_queue
end

def sbatch(command)
    output = `sbatch #{command} 2>&1`
    # Sometimes the queue system needs a little time to think.
    while output =~ /error/
        STDERR.puts "Slurm error, waiting 10s"
        sleep 10
        output = `sbatch #{command} 2>&1`
    end
    return output
end

def mins2hms(mins)
    # The following code uses seconds, so convert from minutes.
    t = (mins*60).to_i
    "%02d:%02d:%02d" % [t/86400*24 + t/3600%24, t/60%60, t%60]
end

def write(file:, contents:)
    begin
        File.open(file, 'w') { |f| f.puts contents }
    rescue
        STDERR.puts "Could not write to #{file} !"
        exit 1
    end
end

def obtain_obsids(argv)
    # This function parses inputs, determining if they're actually obsids or
    # files containing obsids.
    def check_and_push(obsids, obsid)  
        return if obsid.strip.empty? or obsid.to_i == 0
        obsids.push(obsid.to_i)
    end

    obsids = []
    argv.each do |o|
        # Check if this argument is a file - if so, assume it contains obsids.
        if File.file?(o)
            File.readlines(o).each do |o2|
                check_and_push(obsids, o2)
            end
        # Otherwise, assume the argument is an obsid and add it.
        else
            check_and_push(obsids, o)
        end
    end
    obsids
end

def read_fits_key(fits:, key:)
    match = File.open(fits, 'r') { |f| f.read(10000).match(/#{key}\s*=\s*(\S+)/) }
    begin
        return match[1]
    rescue
        STDERR.puts "#{fits} does not have a #{key} key!"
        exit 1
    end
end

def alter_config(text:, key:, value:)
    # Comments come as two forward slashes (i.e. //)
    # before a key value.
    results = text.scan(/(..)?(#{key}.*\n)/)

    # If there are multiple of the samekeys, remove all but the last.
    if results.length > 1
        results.each_with_index do |r, i|
            if i < results.length - 1
                text.sub!(r.join(''), '')
            end
        end
    end

    text.sub!(results.last.join(''), "#{key}=#{value}\n")
end

def generate_slurm_header(job_name:, machine:, partition:, mins:, nodes:, ntasks_per_node: 1, output: nil)
    stdout = output ? output : "#{job_name}-%A.out"
    header = "#!/bin/bash
#SBATCH --job-name=#{job_name}
#SBATCH --output=#{stdout}
#SBATCH --nodes=#{nodes}
#SBATCH --ntasks-per-node=#{ntasks_per_node}
#SBATCH --time=#{mins2hms(mins)}
#SBATCH --clusters=#{machine}
#SBATCH --partition=#{partition}
#SBATCH --account=#{$project}
#SBATCH --export=ALL
"

    header << "#SBATCH --gres=gpu:1" << "\n" if partition == "gpuq"
    header << "set -eux" << "\n"
end

def rts_version(path)
    path = `which #{path}`.chomp unless path.include?('/')
    git_commit_file = path.split('/')[0..-2].join('/') + "/git_commit.txt"
    if File.exists?(git_commit_file)
        git_commit = File.readlines(git_commit_file).join
        "#{path}\n\n#{git_commit}"
    else
        path
    end
end

def flag_tiles
    # A cheap, horrible hack until I can be bothered doing something more appropriate.
    # For now, just look at the bandpass calibration solutions.
    bp_output = `/group/mwaeor/cjordan/software/plot_BPcal_128T.py`
    bp_output.scan(/\(flag\s+(\d+)\?\)/).flatten.uniq.join("\n")
end

def check_rts_status(path: ".")
    stdout_log = Dir.glob("#{path}/RTS*.out").sort_by { |l| File.mtime(l) }.last

    # If there's no log, then maybe the job didn't run - or it is a cotter job.
    if not stdout_log
        stokes_I_filenames=Dir.glob("#{path}/*I.fits")
        if stokes_I_filenames.length >= 50
           status = "peeled"
           final = "24 stokes I images present" 
        else
           status = "???"
           final = "*** no logs"
        end
    # If it's too big, then it failed.
    elsif File.stat(stdout_log).size.to_f > 1000000
        status = "failed"
        final = "*** huge stdout - tiles probably need to be flagged."
    # If there's a line about "could not open...", the data isn't there.
    elsif File.read(stdout_log).match(/Could not open array file for reading/)
        status = "no data"
        final = "*** probably no gpubox files available for this obsid."
    elsif File.read(stdout_log).match(/Error: Unable to set weighted average frequency. No unflagged channels/)
        status = "???"
        final = "Error: Unable to set weighted average frequency. No unflagged channels"
    end

    # Skip to the end if we already have a status.
    unless status
        # Read the latest node???.log file.
        node_log = Dir.glob("#{path}/*node*.log").sort_by { |l| File.mtime(l) }.last
        if not node_log
           stokes_I_filenames=Dir.glob("#{path}/*I.fits")
           if stokes_I_filenames.length >= 50
              status = "peeled"
              final = "24 stokes I images present"
           else
              status = "???"
              final = "*** no node logs"
           end
        else
            # Read the last line of the log.
            final = File.readlines(node_log).last.strip
            if final =~ /LogDone. Closing file/
                status = "peeled"
            else
                status = "failed"
            end
        end
    end

    return status, final
end

# Convert two ionospheric statistics to a single number.
# See Jordan et al. 2017 for more information.
def iono_metric(mag:, pca:)
    m = mag.to_f
    eig = pca.to_f

    metric = 25 * m
    metric += 64 * eig * (eig - 0.6) if eig > 0.6
    return metric.to_s
end

def determine_iono_metrics(obsid:, path:, srclist:)
    # Find the peel log corresponding to the highest frequency.
    peel_logs = Dir.glob("#{path}/rts*node*.log")
    smallest_node = peel_logs.map { |f| f.match(/node(\d{3})/)[1] }.min
    peel_log = peel_logs.select { |f| f.include? "node#{smallest_node}" }
                        .sort_by { |f| File.mtime(f) }
                        .last
    # Run the log through cthulhu.
    mag, pca = `cthulhu_wrapper.py #{peel_log}`.chomp.split.drop(1)
    iono_qa = iono_metric(mag: mag, pca: pca)

    # Update the MWA QA database with the results if you're using a
    # non-standard PGHOST, which implies that you're allowed to write to the DB.
    if not ENV["PGHOST"] == "mwa-metadata01.pawsey.org.au"
        `mwaqa_update_db.py -o #{obsid} \\
                            -p #{path} \\
                            -s #{File.basename(srclist)} \\
                            --iono_mag #{mag} \\
                            --iono_pca #{pca}`
    end

    return mag, pca, iono_qa
end

class Obsid
    attr_reader :obsid,
                :type,
                :path,
                :download_jobid,
                :setup_jobid,
                :patch_jobid,
                :peel_jobid,
                :high_setup_jobid,
                :high_patch_jobid,
                :high_peel_jobid,
                :low_setup_jobid,
                :low_patch_jobid,
                :low_peel_jobid,
                :metafits,
                :timestamp_dir,
                :status,
                :final,
                :rts_path,
                :stdout_log,
                :node001_log,
                :sister_obsid,
                :epoch_id

    def initialize(obsid, path: nil)
        # "obsid" is (probably) a 10 digit string, representing the GPS timestamp
        # of an observation collected with the MWA. If this is a moon observation 
        # then obsid is actually a pair of obsids separated by an '_': obsid_sisterobsid
        @obsid = obsid.to_i
        if path
            @path = path
            FileUtils.mkdir_p @path unless File.directory?(@path)
        elsif @obsid.to_s.length == 20
            first_obsid=@obsid.to_s[0,10]
            @path = "#{$mwa_dir}/data/#{first_obsid}"
            FileUtils.mkdir_p @path unless File.directory?(@path)
        else
            @path = "#{$mwa_dir}/data/#{obsid}" 
            FileUtils.mkdir_p @path unless File.directory?(@path)
        end
        #get the latest metafits file if not paired moon obs
        get_metafits=`wget -O #{@path}/#{obsid}_metafits_ppds.fits http://mwa-metadata01.pawsey.org.au/metadata/fits?obs_id=#{obsid}` unless @obsid.to_s.length == 20
        #get the latest metafits file if not paired moon obs
        get_metafits=`wget -O #{@path}/#{first_obsid}_metafits_ppds.fits http://mwa-metadata01.pawsey.org.au/metadata/fits?obs_id=#{first_obsid}` if @obsid.to_s.length == 20
        @metafits = Dir.glob("#{@path}/*metafits*").sort_by { |f| File.size(f) }.last
    end

    def obs_type
        # Use the RAPHASE header tag wherever available.
        ra = read_fits_key(fits: @metafits, key: "RAPHASE").to_f
        filename = read_fits_key(fits: @metafits, key: "FILENAME")

        # EoR-specific fields.
        if read_fits_key(fits: @metafits, key: "PROJECT").include? "G0009" or
          read_fits_key(fits: @metafits, key: "GRIDNAME").include? "EOR" or
          read_fits_key(fits: @metafits, key: "PROJECT").include? "D0000"
            if ra.close_to?(0, tol: 5) or ra.close_to?(360, tol: 5)
                @type = "EOR0"
            elsif ra.close_to?(60, tol: 5)
                @type = "EOR1"
            elsif ra.close_to?(155, tol: 5)
                @type = "EOR2"
            elsif filename.include? "LymanA"
                @type = "LymanA"
            elsif filename.include? "CenA"
                @type = "CenA"
            elsif filename.include? "HydA"
                @type = "HydA"
            else
                @type = "RA=#{ra}"
            end
        elsif read_fits_key(fits: @metafits, key: "PROJECT").include? "G0017"
            @type = "moon"
        #CenA obs 
        elsif filename.include? "CenA"
            @type = "CenA"
        #HydA obs 
        elsif filename.include? "HydA"
            @type = "HydA"
        # Everything else.
        else
            @type = "RA=#{ra}"
        end
    end

    def download(mins: 30)
        contents = generate_slurm_header(job_name: "dl_#{@obsid}",
                                         machine: "zeus",
                                         partition: "copyq",
                                         mins: mins,
                                         nodes: 1,
                                         output: "getNGASdata-#{@obsid}-%A.out")
        contents << "
cd #{$mwa_dir}/data
obsdownload.py -o #{@obsid} --chstart=1 --chcount=24 -f -m
"

        FileUtils.mkdir_p @path unless Dir.pwd == @path
        Dir.chdir @path unless Dir.pwd == @path
        write(file: "#{@obsid}.sh", contents: contents)
        @download_jobid = sbatch("#{@obsid}.sh").match(/Submitted batch job (\d+)/)[1].to_i
    end

    def rts(setup_mins: 10,
            cal_mins: 40,
            patch: true,
            peel: true,
            cotter: false,
            ms_download: false,
            cotter_only: false,
            peel_number: 1000,
            timestamp: true,
            srclist: "#{ENV["SRCLIST_ROOT"]}/srclist_pumav3_EoR0aegean_EoR1pietro+ForA.txt",
            rts_path: "rts_gpu",
            sister_obsid: null,
            epoch_id: null)
        if peel and not patch
            abort "Cannot peel if we are not patching; exiting."
        end
        @patch = patch
        @peel = peel
        @cotter = cotter
        @ms_download = ms_download
        @cotter_only = cotter_only
        @sister_obsid = sister_obsid
        @epoch_id = epoch_id

        @metafits = Dir.glob("#{@path}/*metafits*").sort_by { |f| File.size(f) }.last
        abort("#{@obsid}: metafits file not found!") unless metafits
        @int_time = read_fits_key(fits: metafits, key: "INTTIME")

        @peel_number = peel_number
        @rts_path = rts_path

        if @int_time == "0.5"
            @corr_dumps_per_cadence_cal = 128
            @number_of_integration_bins_cal = 7

            @corr_dumps_per_cadence_peel = 16
            @number_of_integration_bins_peel = 5
        elsif @int_time == "1.0"
            @corr_dumps_per_cadence_cal = 64
            @number_of_integration_bins_cal = 7

            @corr_dumps_per_cadence_peel = 8
            @number_of_integration_bins_peel = 3
        elsif @int_time == "2.0"
            @corr_dumps_per_cadence_cal = 32
            @number_of_integration_bins_cal = 6

            @corr_dumps_per_cadence_peel = 4
            @number_of_integration_bins_peel = 3
        else
            abort(sprintf "Unknown integration time! (%s for %s)", @int_time, @obsid)
        end
        @number_of_iterations_cal = 1
        @number_of_iterations_peel = 14

        if timestamp
            date, time = Time.now.to_s.split[0..1]
            @timestamp_dir = [date, time.split(':')[0..1].join].join('_')
        else
            @timestamp_dir = ""
        end

        @source_list = srclist
        source_list_prefix = @source_list.split('/').last.split(".txt").first
        @patch_source_catalogue_file = "#{@path}/#{timestamp_dir}/#{source_list_prefix}_#{@obsid}_patch1000.txt"
        @peel_source_catalogue_file = "#{@path}/#{timestamp_dir}/#{source_list_prefix}_#{@obsid}_peel3000.txt"

        # The RA needs to be in an hour angle format.
        @obs_image_centre_ra = (read_fits_key(fits: @metafits, key: "RAPHASE").to_f / 15.0).to_s
        @obs_image_centre_dec = read_fits_key(fits: @metafits, key: "DECPHASE")

        # Run the "obs_type" function if the "type" attribute doesn't exist.
        obs_type unless @type
        # Here, we handle special observations. If the current "type" isn't listed here, it gets processed generically.
        if @type == "LymanA"
            # High band
            # Frequency of channel 24, if all bands were contiguous (identical here, i.e. channel 142)
            # This frequency is actually about half a channel lower in frequency, i.e. 141.49609*1.28MHz
            @obs_freq_base = 181.135
            @subband_ids = (17..24).to_a.join(',')
            @timestamp_dir << "_high"
            @patch_source_catalogue_file = "#{@path}/#{timestamp_dir}/#{source_list_prefix}_#{@obsid}_patch1000.txt"
            @peel_source_catalogue_file = "#{@path}/#{timestamp_dir}/#{source_list_prefix}_#{@obsid}_peel3000.txt"
            rts_setup(mins: setup_mins)
            rts_patch(mins: cal_mins, peel: @peel) if @patch
            @high_setup_jobid = @setup_jobid
            @high_patch_jobid = @patch_jobid

            # Low band
            # Frequency of channel 24, if all bands were contiguous (channel 150 here, even though our lowest is 158)
            # This frequency is actually about half a channel lower in frequency, i.e. 149.49609*1.28MHz
            @obs_freq_base = 191.355
            @subband_ids = (1..16).to_a.join(',')
            @timestamp_dir = @timestamp_dir.split('_').select { |e| e =~ /\d/ }.join('_') << "_low"
            @patch_source_catalogue_file = "#{@path}/#{timestamp_dir}/#{source_list_prefix}_#{@obsid}_patch1000.txt"
            @peel_source_catalogue_file = "#{@path}/#{timestamp_dir}/#{source_list_prefix}_#{@obsid}_peel3000.txt"
            rts_setup(mins: setup_mins)
            rts_patch(mins: cal_mins, peel: @peel) if @patch
            @low_setup_jobid = @setup_jobid
            @low_patch_jobid = @patch_jobid
        else
            @channel_bandwidth = 0.02 if @type == "EOR2" and @int_time == "2.0" 
            # This is for all other "non-special" fields, including EoR fields.
            @subband_ids = (1..24).to_a.join(',')
            rts_setup(mins: setup_mins)
            if @ms_download
               ms_download()
            else
               rts_patch(mins: cal_mins, peel: @peel) if @patch
            end
        end
    end

    def rts_setup(mins: 10,
                  ben_code_base: "/astro/mwaeor/bmckinley/code/"
                  )
        @ben_code_base = ben_code_base
        @have_beam_string = ""
        @main_obsid = @obsid.to_s[0,10]
        if @type == "EOR2"
            @epoch_id = "2014A_EoR2_gp13"
            @srclist_code_base = "/group/mwa/software/srclists/master/"
            @sourcelist = "srclist_pumav3_EoR0aegean_EoR1pietro+ForA.txt" 
            @sister_obsid_infile_string = ""
            @no_dysco_string = "--no_dysco"
            @channels_out_string = '--channels_out=24'
            @ionpeeled_string = "--ionpeeled"
        elsif @type == "moon"
            #@sister_obsid = @obsid.to_s[10...20] 
            puts "OBSID and SISTER OBSID:"
            puts @obsid.to_s
            puts @sister_obsid
            @srclist_code_base = "/group/mwa/software/srclists/master/"
            @sourcelist = "srclist_pumav3_EoR0aegean_EoR1pietro+ForA.txt"
            @no_dysco_string = ""
            @ionpeeled_string = ""
        elsif @type == "CenA"
            @srclist_code_base = "/group/mwa/software/anoko/mwa-reduce/models/"
            @sourcelist = "model-CenA-50comp_withalpha.txt"
            @sister_obsid_infile_string = ""
            @no_dysco_string = ""
            @ionpeeled_string = ""
        elsif @type == "HydA"
            @srclist_code_base = "/group/mwa/software/anoko/mwa-reduce/models/"
            @sourcelist = "model-HydA-58comp_withalpha.txt"
            @sister_obsid_infile_string = ""
            @no_dysco_string = ""
            @ionpeeled_string = ""
        else 
            @srclist_code_base = "/group/mwa/software/srclists/master/"
            @sourcelist = "srclist_pumav3_EoR0aegean_EoR1pietro+ForA.txt"
            @sister_obsid_infile_string = ""
            @no_dysco_string = ""
            @ionpeeled_string = ""
        end
        #things that depend on epoch_ID
        if @epoch_id=='2015B_05'
           @flag_ants_string = "'56 60'"
        else
           @flag_ants_string = "''"
        end

        #things that depend on obs semester
        obs_semester=@epoch_id.to_s[0,5]
        puts obs_semester 
        if obs_semester=='2015A' or obs_semester=='2015B'
            @cleanup_string=''
            time_averaging='8'
            freq_averaging='80'
            imsize_string='--imsize=2048'
            if @type == "moon" 
                wsclean_options_string='--wsclean_options=" -niter 0 -datacolumn CORRECTED_DATA  -scale 0.0085 -weight uniform  -smallinversion  -channelsout 24 -make-psf  "'
            else
                wsclean_options_string='--wsclean_options=" -niter 2000 -threshold 1.5 -multiscale -mgain 0.85 -joinpolarizations -datacolumn CORRECTED_DATA  -scale 0.0085 -weight briggs 0  -smallinversion  -channelsout 1 -make-psf  "'
            end

        elsif obs_semester=='2017B' or obs_semester=='2018A'
            @cleanup_string='--cleanup'
            time_averaging='4'
            freq_averaging='40'
            imsize_string='--imsize=4096'
            if @type == "moon"
                wsclean_options_string='" -niter 0  -datacolumn CORRECTED_DATA  -scale 0.0042 -weight natural  -smallinversion -channelsout 24 -make-psf "'
            else
                @cleanup_string=''
                wsclean_options_string='--wsclean_options=" -niter 2000 -threshold 1.5 -multiscale -mgain 0.85 -joinpolarizations -datacolumn CORRECTED_DATA  -scale 0.0042 -weight natural  -smallinversion  -channelsout 1 -make-psf  "'
            end
        else
            puts "observing semester %s not known" % obs_semester
        end
        contents = generate_slurm_header(job_name: "se_#{@obsid}",
                                         machine: "galaxy",
                                         partition: "gpuq",
                                         mins: mins,
                                         nodes: 1,
                                         output: "RTS-setup-#{@obsid}-%A.out")
        contents << "
echo #{@main_obsid} > #{@main_obsid}.txt
" if @cotter
        contents << "
echo #{@sister_obsid} > #{@sister_obsid}.txt
" if @type == "moon" and @cotter
        contents << "
#generate_cotter on moon
#python #{@ben_code_base}ben-astronomy/moon/processing_scripts/namorrodor_magnus/generate_cotter_moon.py \\
#                   --epoch_ID=#{@epoch_id} \\
#                   --flag_ants='' \\
#                   --obsid_infile=${PWD}/#{@main_obsid}.txt \\
#                   --sister_obsid_infile=${PWD}/#{@sister_obsid}.txt \\
#                   --track_moon \\
#                   #{@no_dysco_string} \\
#                   #{@cleanup_string} \\
#                   /
#generate manta_ray on moon
python #{@ben_code_base}ben-astronomy/moon/processing_scripts/namorrodor_magnus/generate_manta_ray.py \\
                   --epoch_ID=#{@epoch_id} \\
                   --flag_ants='' \\
                   --obsid_infile=${PWD}/#{@main_obsid}.txt \\
                   --sister_obsid_infile=${PWD}/#{@sister_obsid}.txt \\
                   --track_moon \\
                   / 
" if @type == "moon" and @cotter
        contents << "
##generate_cotter for sister obsid (off moon)
#python #{@ben_code_base}ben-astronomy/moon/processing_scripts/namorrodor_magnus/generate_cotter_moon.py \\
#                   --epoch_ID=#{@epoch_id} \\
#                   --flag_ants='' \\
#                   --obsid_infile=${PWD}/#{@sister_obsid}.txt \\
#                   --sister_obsid_infile=${PWD}/#{@main_obsid}.txt \\
#                   --track_off_moon=#{@path}/track_off_moon_#{@main_obsid}_#{@sister_obsid}.txt \\
#                   #{@no_dysco_string} \\
#                   #{@cleanup_string} \\
#                   /
#generate manta ray for sister obsid (off moon)
python #{@ben_code_base}ben-astronomy/moon/processing_scripts/namorrodor_magnus/generate_manta_ray.py \\
                   --epoch_ID=#{@epoch_id} \\
                   --flag_ants='' \\
                   --obsid_infile=${PWD}/#{@sister_obsid}.txt \\
                   --sister_obsid_infile=${PWD}/#{@main_obsid}.txt \\
                   --track_off_moon=#{@path}/track_off_moon_#{@main_obsid}_#{@sister_obsid}.txt \\
                   /
" if @type == "moon" and @cotter
        contents << "
##generate_cotter for unmoon obs
#python #{@ben_code_base}ben-astronomy/moon/processing_scripts/namorrodor_magnus/generate_cotter_moon.py \\
#                   --epoch_ID=#{@epoch_id} \\
#                   --flag_ants='' \\
#                   --obsid_infile=${PWD}/#{@main_obsid}.txt \\
#                   #{@no_dysco_string} \\
#                   #{@cleanup_string} \\
#                   /
#generate manta ray for unmoon obs
python #{@ben_code_base}ben-astronomy/moon/processing_scripts/namorrodor_magnus/generate_manta_ray.py \\
                   --epoch_ID=#{@epoch_id} \\
                   --flag_ants='' \\
                   --obsid_infile=${PWD}/#{@main_obsid}.txt \\
                   /
" if @cotter unless @type == "moon"
        contents << " 
#generate_selfcal on moon
python #{@ben_code_base}ben-astronomy/moon/processing_scripts/namorrodor_magnus/generate_qselfcal_concat_ms.py \\
                   --epoch_ID=#{@epoch_id} \\
                   --track_moon \\
                   --sourcelist=#{@srclist_code_base}#{@sourcelist} \\
                   --flag_ants=#{@flag_ants_string} \\
                   --cotter \\
                   --selfcal=0 \\
                   --obsid_infile=${PWD}/#{@main_obsid}.txt \\
                   --sister_obsid_infile=${PWD}/#{@sister_obsid}.txt \\
                   /
" if @type == "moon" and @cotter unless @cotter_only
        contents << " 
#generate_selfcal sister obsid (off moon)
python #{@ben_code_base}ben-astronomy/moon/processing_scripts/namorrodor_magnus/generate_qselfcal_concat_ms.py \\
                   --epoch_ID=#{@epoch_id} \\
                   --track_off_moon=#{@path}/track_off_moon_#{@main_obsid}_#{@sister_obsid}.txt \\
                   --sourcelist=#{@srclist_code_base}#{@sourcelist} \\
                   --flag_ants=#{@flag_ants_string} \\
                   --cotter \\
                   --selfcal=0 \\
                   --obsid_infile=${PWD}/#{@sister_obsid}.txt \\
                   --sister_obsid_infile=${PWD}/#{@main_obsid}.txt \\
                   /
" if @type == "moon" and @cotter unless @cotter_only
        contents << " 
#generate_selfcal unmoon obs
python #{@ben_code_base}ben-astronomy/moon/processing_scripts/namorrodor_magnus/generate_qselfcal_concat_ms.py \\
                   --epoch_ID=#{@epoch_id} \\
                   --sourcelist=#{@srclist_code_base}#{@sourcelist} \\
                   --cotter \\
                   --flag_ants=#{@flag_ants_string} \\
                   --selfcal=0 \\
                   --obsid_infile=${PWD}/#{@main_obsid}.txt \\
                   /
" if @cotter unless @type == "moon" unless @cotter_only
        contents << "
#generate peel 
python #{@ben_code_base}ben-astronomy/moon/processing_scripts/namorrodor_magnus/generate_qselfcal_concat_ms.py \\
                   --epoch_ID=#{@epoch_id} \\
                   --ionpeel=#{@srclist_code_base}#{@sourcelist} \\
                   --cotter \\
                   --selfcal=0 \\
                   --obsid_infile=${PWD}/#{@main_obsid}.txt \\
                   #{@sister_obsid_infile_string} \\
                   /
#generate_export_uvfits
python #{@ben_code_base}ben-astronomy/moon/processing_scripts/namorrodor_magnus/generate_export_uvfits.py \\
                   --epoch_ID=#{@epoch_id} \\
                   --obsid_infile=${PWD}/#{@main_obsid}.txt \\
                   #{@ionpeeled_string} \\
                   #{@channels_out_string} \\
                   /
" if @cotter and @type == "EOR2" unless @cotter_only
        contents << "
#generate_image on moon
python #{@ben_code_base}ben-astronomy/moon/processing_scripts/namorrodor_magnus/generate_mwac_qimage_concat_ms.py \\
                   --epoch_ID=#{@epoch_id} \\
                   --cotter \\
                   --crop_images \\
                   --no_pbcorr \\
                   --pol='xx,xy,yx,yy' \\
                   #{imsize_string} \\
                   #{wsclean_options_string} \\
                   --obsid_infile=${PWD}/#{@main_obsid}.txt \\
                   --track_moon \\
                   #{@ionpeeled_string} \\
                   /
" if @cotter and @type == "moon" unless @cotter_only
        contents << "
#generate_image sister ob (off moon)
python #{@ben_code_base}ben-astronomy/moon/processing_scripts/namorrodor_magnus/generate_mwac_qimage_concat_ms.py \\
                   --epoch_ID=#{@epoch_id} \\
                   --cotter \\
                   --crop_images \\
                   --no_pbcorr \\
                   --pol='xx,xy,yx,yy' \\
                   #{imsize_string} \\
                   #{wsclean_options_string} \\
                   --obsid_infile=${PWD}/#{@sister_obsid}.txt \\
                   --track_off_moon=#{@path}/track_off_moon_#{@main_obsid}_#{@sister_obsid}.txt \\
                   #{@ionpeeled_string} \\
                   /
" if @cotter and @type == "moon"  unless @cotter_only
        contents << "
#generate_image unmoon obs
python #{@ben_code_base}ben-astronomy/moon/processing_scripts/namorrodor_magnus/generate_mwac_qimage_concat_ms.py \\
                   --epoch_ID=#{@epoch_id} \\
                   --cotter \\
                   --no_pbcorr \\
                   --pol='xx,xy,yx,yy' \\
                   #{imsize_string} \\
                   #{wsclean_options_string} \\
                   --obsid_infile=${PWD}/#{@main_obsid}.txt \\
                   #{@ionpeeled_string} \\
                   /
" if @cotter unless @type == "moon" unless @cotter_only
        contents << "
#generate_pbcorr on moon
python #{@ben_code_base}ben-astronomy/moon/processing_scripts/namorrodor_magnus/generate_qpbcorr_multi.py  \\
                   --epoch_ID=#{@epoch_id} \\
                   --track_moon \\
                   --crop_images \\
                   --obsid_infile=${PWD}/#{@main_obsid}.txt \\
                   --channelsout=24 \\
                   #{@have_beam_string} \\
                   #{@ionpeeled_string} \\
                   --array_by_chan \\
                   /
" if @cotter and @type == "moon" unless @cotter_only
        contents << "
#generate_pbcorr sister obs (off moon)
python #{@ben_code_base}ben-astronomy/moon/processing_scripts/namorrodor_magnus/generate_qpbcorr_multi.py  \\
                   --epoch_ID=#{@epoch_id} \\
                   --track_off_moon=#{@path}/track_off_moon_#{@main_obsid}_#{@sister_obsid}.txt \\
                   --obsid_infile=${PWD}/#{@sister_obsid}.txt \\
                   --crop_images \\
                   --channelsout=24 \\
                   #{@have_beam_string} \\
                   #{@ionpeeled_string} \\
                   --array_by_chan \\
                   /
" if @cotter and @type == "moon" unless @cotter_only
        contents << "
#generate_pbcorr unmoon obs
python #{@ben_code_base}ben-astronomy/moon/processing_scripts/namorrodor_magnus/generate_qpbcorr_multi.py  \\
                   --epoch_ID=#{@epoch_id} \\
                   --obsid_infile=${PWD}/#{@main_obsid}.txt \\
                   --channelsout=24 \\
                   #{@have_beam_string} \\
                   #{@ionpeeled_string} \\
                   --array_by_chan \\
                   /
" if @cotter unless @type == "moon" unless @cotter_only
        contents << "
list_gpubox_files.py obsid.dat
ln -sf ../gpufiles_list.dat .

#####
# new way / getting rid off the wrapper generate_dynamic_RTS_sourcelists.py
#
# two commands feeding directly srclist_by_beam.py to pick randomly sources and attenuate them
# by the beam. one to feed the patch (calibrate data) and one to feed in peel (peel sources from data)
#####
srclist_by_beam.py -n 1000 \\
                   --srclist=#{@source_list} \\
                   --metafits=#{@metafits} \\
                   --order=\"distance\"
" unless @cotter 
        contents << "
srclist_by_beam.py -n 3000 \\
                   --srclist=#{@source_list} \\
                   --metafits=#{@metafits} \\
                   --order=\"distance\" \\
                   --no_patch \\
                   --cutoff=30
" if @peel unless @cotter
        contents << "
#####

#####
# generate the rts_patch.sh file - to many options and stuff. should be more like rts_setup!
generate_mwac_qRTS_auto.py #{@path}/#{@timestamp_dir}/obsid.dat \\
                           #{ENV["USER"]} 24 \\
                           /group/mwaeor/bpindor/templates/EOR0_selfCalandPeel_PUMA1000_WriteUV_80khz_cotter_template.dat \\
                           --auto \\
                           --chunk_number=0 \\
                           --dynamic_sourcelist=1000 \\
                           --sourcelist=#{@source_list}
#####

reflag_mwaf_files.py #{@path}/#{@timestamp_dir}/obsid.dat

#####
# the following should be replaced by a template generation function from metafits header.
generate_RTS_in_mwac.py #{@path} \\
                        #{ENV["USER"]} 24 128T \\
                        --templates=/group/mwaeor/bpindor/templates/EOR0_selfCalandPeel_PUMA1000_WriteUV_80khz_cotter_template.dat \\
                        --header=#{@metafits} \\
                        --channel_flags=/group/mwaeor/bpindor/templates/flagged_channels_default.txt

sed -i \"s|\\(CorrDumpsPerCadence=\\).*|\\1#{@corr_dumps_per_cadence_cal}|\" #{ENV["USER"]}_rts_0.in
sed -i \"s|\\(NumberOfIntegrationBins=\\).*|\\1#{@number_of_integration_bins_cal}|\" #{ENV["USER"]}_rts_0.in
sed -i \"s|\\(NumberOfIterations=\\).*|\\1#{@number_of_iterations_cal}|\" #{ENV["USER"]}_rts_0.in
sed -i \"s|//SourceCatalogueFile.*||; s|\\(SourceCatalogueFile=\\).*|\\1#{@patch_source_catalogue_file}|\" #{ENV["USER"]}_rts_0.in
sed -i \"s|\\(doRFIflagging=\\).*|\\11|\" #{ENV["USER"]}_rts_0.in
sed -i \"s|\\(ObservationImageCentreRA=\\).*|\\1#{@obs_image_centre_ra}|\" #{ENV["USER"]}_rts_0.in
sed -i \"s|\\(ObservationImageCentreDec=\\).*|\\1#{@obs_image_centre_dec}|\" #{ENV["USER"]}_rts_0.in
sed -i \"s|\\(SubBandIDs=\\).*|\\1#{@subband_ids}|\" #{ENV["USER"]}_rts_0.in

sed -i \"s|\\(CorrDumpsPerCadence=\\).*|\\1#{@corr_dumps_per_cadence_peel}|\" #{ENV["USER"]}_rts_1.in
sed -i \"s|\\(NumberOfIntegrationBins=\\).*|\\1#{@number_of_integration_bins_peel}|\" #{ENV["USER"]}_rts_1.in
sed -i \"s|\\(NumberOfIterations=\\).*|\\1#{@number_of_iterations_peel}|\" #{ENV["USER"]}_rts_1.in
sed -i \"s|//SourceCatalogueFile.*||; s|\\(SourceCatalogueFile=\\).*|\\1#{@peel_source_catalogue_file}|\" #{ENV["USER"]}_rts_1.in
sed -i \"s|\\(doRFIflagging=\\).*|\\11|\" #{ENV["USER"]}_rts_1.in
sed -i \"s|\\(ObservationImageCentreRA=\\).*|\\1#{@obs_image_centre_ra}|\" #{ENV["USER"]}_rts_1.in
sed -i \"s|\\(ObservationImageCentreDec=\\).*|\\1#{@obs_image_centre_dec}|\" #{ENV["USER"]}_rts_1.in
sed -i \"s|\\(SubBandIDs=\\).*|\\1#{@subband_ids}|\" #{ENV["USER"]}_rts_1.in
sed -i \"s|\\(NumberOfSourcesToPeel=\\).*|\\1#{@peel_number}|\" #{ENV["USER"]}_rts_1.in
sed -i \"s|\\(NumberOfIonoCalibrators=\\).*|\\1#{@peel_number}|\" #{ENV["USER"]}_rts_1.in
" unless @cotter
        contents << "
sed -i \"s|\\(ObservationFrequencyBase=\\).*|\\1#{@obs_freq_base}|\" #{ENV["USER"]}_rts_0.in
sed -i \"s|\\(ObservationFrequencyBase=\\).*|\\1#{@obs_freq_base}|\" #{ENV["USER"]}_rts_1.in
" if @obs_freq_base unless @cotter

#On 2014-03-02 the correlator settings for EoR2 change from 40 kHz, 0.5 s to 20 kHz, 2 s
#Bart's script takes care of the NumberOfChannels but not ChannelBandwidth, which must be set to 20 kHz
        contents << "
sed -i \"s|\\(ChannelBandwidth=\\).*|\\1#{@channel_bandwidth}|\" #{ENV["USER"]}_rts_0.in
sed -i \"s|\\(ChannelBandwidth=\\).*|\\1#{@channel_bandwidth}|\" #{ENV["USER"]}_rts_1.in
" if @channel_bandwidth unless @cotter

        Dir.chdir @path unless Dir.pwd == @path
        FileUtils.mkdir_p @timestamp_dir
        Dir.chdir @timestamp_dir
        system("ln -sf ../*metafits* .")
        write(file: "obsid.dat", contents: @obsid)
        write(file: "rts_setup.sh", contents: contents)
        @setup_jobid = sbatch("rts_setup.sh").match(/Submitted batch job (\d+)/)[1].to_i
    end

    def ms_download()
       if @type == "moon"
          p manta_ray_filename = "q_manta_ray_on_moon_0.sh"
          p @patch_jobid = sbatch("--dependency=afterok:#{@setup_jobid} -M zeus --partition=copyq #{manta_ray_filename}").match(/Submitted batch job (\d+)/)[1].to_i
          p manta_ray_filename = "q_manta_ray_off_moon_0.sh"
          p @patch_jobid = sbatch("--dependency=afterok:#{@setup_jobid} -M zeus --partition=copyq #{manta_ray_filename}").match(/Submitted batch job (\d+)/)[1].to_i
       else
          manta_ray_filename = "q_manta_ray_moon_0.sh"
          @patch_jobid = sbatch("--dependency=afterok:#{@setup_jobid} #{manta_ray_filename}").match(/Submitted batch job (\d+)/)[1].to_i
       end
    end

    def rts_patch(mins: 15, peel: false)
        if !@cotter
            # Peel is allowed to be redefined here for more flexibility.
   
            num_nodes = @subband_ids.split(',').length + 1

            filename = "rts_patch.sh"
            contents = generate_slurm_header(job_name: "pa_#{@obsid}",
                                             machine: "galaxy",
                                             partition: "gpuq",
                                             mins: mins,
                                             nodes: num_nodes,
                                             output: "RTS-patch-#{@obsid}-%A.out")
            contents << "
srun -n #{num_nodes} #{@rts_path} #{ENV["USER"]}_rts_0.in

/group/mwaeor/cjordan/software/plot_BPcal_128T.py
/group/mwaeor/cjordan/software/plot_CalSols.py --base_dir=`pwd` -n #{@obsid} -i
touch flagged_tiles.txt

srun -n #{num_nodes} #{@rts_path} #{ENV["USER"]}_rts_0.in
" 
            if peel
                contents << "srun -n #{num_nodes} #{@rts_path} #{ENV["USER"]}_rts_1.in\n"
            end

            Dir.chdir "#{@path}/#{@timestamp_dir}" unless Dir.pwd == "#{@path}/#{@timestamp_dir}"
            write(file: filename, contents: contents)
            write(file: "rts_version_used.txt", contents: rts_version(@rts_path))
            @patch_jobid = sbatch("--dependency=afterok:#{@setup_jobid} #{filename}").match(/Submitted batch job (\d+)/)[1].to_i
        else
            Dir.chdir "#{@path}/#{@timestamp_dir}" unless Dir.pwd == "#{@path}/#{@timestamp_dir}"
            #cotter_filename = "q_cotter_moon_0.sh" unless @type == "moon"
            #cotter_filename = "q_cotter_on_moon_0.sh" if @type == "moon"
            #@patch_jobid = sbatch("--dependency=afterok:#{@setup_jobid} #{cotter_filename}").match(/Submitted batch job (\d+)/)[1].to_i
            #cotter_filename = "q_cotter_off_moon_0.sh" if @type == "moon"
            #@patch_jobid = sbatch("--dependency=afterok:#{@setup_jobid} #{cotter_filename}").match(/Submitted batch job (\d+)/)[1].to_i if @type == "moon"
            if @type == "moon"
               ##do all this downloading stuff separately...
               #p manta_ray_filename = "q_manta_ray_on_moon_0.sh"
               #p @patch_jobid_on_moon = sbatch("--dependency=afterok:#{@setup_jobid} -M zeus --partition=copyq #{manta_ray_filename}").match(/Submitted batch job (\d+)/)[1].to_i
               #p manta_ray_outfile_on_moon = "manta_ray-#{@patch_jobid_on_moon}.out"
               #p manta_ray_filename = "q_manta_ray_off_moon_0.sh"
               #p @patch_jobid_off_moon = sbatch("--dependency=afterok:#{@setup_jobid} -M zeus --partition=copyq #{manta_ray_filename}").match(/Submitted batch job (\d+)/)[1].to_i
               #p manta_ray_outfile_off_moon = "manta_ray-#{@patch_jobid_off_moon}.out"
               #p print_string="waiting for ms downloads"
               #outfiles_exist = false
               #while !outfiles_exist
               #   outfiles_exist = (File.file?(manta_ray_outfile_on_moon) and File.file?(manta_ray_outfile_off_moon))
               #   #sleep(1) until outfiles_exist
               #end
               #downloads_complete = false
               #while !downloads_complete
               #   downloads_complete = (File.readlines(manta_ray_outfile_on_moon).grep(/mwa_client finished successfully/).any? and File.readlines(manta_ray_outfile_off_moon).grep(/mwa_client finished successfully/).any?)
               #   #sleep(1) until outfiles_exist
               #end
               p selfcal_filename = "q_selfcal_on_moon.sh"
               p @patch_jobid = sbatch("#{selfcal_filename}").match(/Submitted batch job (\d+)/)[1].to_i
               p selfcal_filename = "q_selfcal_off_moon.sh"
               p @patch_jobid = sbatch("--dependency=afterok:#{@patch_jobid} #{selfcal_filename}").match(/Submitted batch job (\d+)/)[1].to_i
               #ionpeel_filename = "q_ionpeel_on_moon.sh"
               #ionpeel_filename = "q_ionpeel_off_moon.sh" if @type == "moon" 
               #@patch_jobid = sbatch("--dependency=afterok:#{@patch_jobid} #{ionpeel_filename}").match(/Submitted batch job (\d+)/)[1].to_i 
               p image_filename = "q_image_on_moon.sh"
               p @patch_jobid = sbatch("--dependency=afterok:#{@patch_jobid} #{image_filename}").match(/Submitted batch job (\d+)/)[1].to_i
               p image_filename = "q_image_off_moon.sh"
               p @patch_jobid = sbatch("--dependency=afterok:#{@patch_jobid} #{image_filename}").match(/Submitted batch job (\d+)/)[1].to_i
               p pbcorr_filename = "q_pbcorr_on_moon.sh"
               p @patch_jobid = sbatch("--dependency=afterok:#{@patch_jobid} #{pbcorr_filename}").match(/Submitted batch job (\d+)/)[1].to_i
               p pbcorr_filename = "q_pbcorr_off_moon.sh"
               p @patch_jobid = sbatch("--dependency=afterok:#{@patch_jobid} #{pbcorr_filename}").match(/Submitted batch job (\d+)/)[1].to_i
               ###
            else
               manta_ray_filename = "q_manta_ray_moon_0.sh"
               @patch_jobid = sbatch("--dependency=afterok:#{@setup_jobid} #{manta_ray_filename}").match(/Submitted batch job (\d+)/)[1].to_i
               selfcal_filename = "q_selfcal_moon.sh"
               @patch_jobid = sbatch("--dependency=afterok:#{@patch_jobid} #{selfcal_filename}").match(/Submitted batch job (\d+)/)[1].to_i
               ionpeel_filename = "q_ionpeel_moon.sh"
               @patch_jobid = sbatch("--dependency=afterok:#{@patch_jobid} #{ionpeel_filename}").match(/Submitted batch job (\d+)/)[1].to_i if @type=="EOR2"
               #ionpeel_filename = "q_ionpeel_off_moon.sh" if @type == "moon" 
               #@patch_jobid = sbatch("--dependency=afterok:#{@patch_jobid} #{ionpeel_filename}").match(/Submitted batch job (\d+)/)[1].to_i 
               export_uvfits_filename = "q_export_uvfits_0.sh"
               @patch_jobid = sbatch("--dependency=afterok:#{@patch_jobid} #{export_uvfits_filename}").match(/Submitted batch job (\d+)/)[1].to_i if @type=="EOR2"
               end
            ##old
            #manta_ray_filename = "q_manta_ray_moon_0.sh" unless @type == "moon"
            #manta_ray_filename = "q_manta_ray_on_moon_0.sh" if @type == "moon"
            #@patch_jobid = sbatch("--dependency=afterok:#{@setup_jobid} #{manta_ray_filename}").match(/Submitted batch job (\d+)/)[1].to_i
            #manta_ray_filename = "q_manta_ray_off_moon_0.sh" if @type == "moon"
            #@patch_jobid = sbatch("--dependency=afterok:#{@patch_jobid} #{manta_ray_filename}").match(/Submitted batch job (\d+)/)[1].to_i if @type == "moon"
            #selfcal_filename = "q_selfcal_moon.sh" unless @type == "moon"
            #selfcal_filename = "q_selfcal_on_moon.sh" if @type == "moon"
            #@patch_jobid = sbatch("--dependency=afterok:#{@patch_jobid} #{selfcal_filename}").match(/Submitted batch job (\d+)/)[1].to_i
            #selfcal_filename = "q_selfcal_off_moon.sh" if @type == "moon"
            #@patch_jobid = sbatch("--dependency=afterok:#{@patch_jobid} #{selfcal_filename}").match(/Submitted batch job (\d+)/)[1].to_i if @type == "moon"
            #ionpeel_filename = "q_ionpeel_moon.sh" unless @type == "moon"
            #ionpeel_filename = "q_ionpeel_on_moon.sh" if @type == "moon"
            #@patch_jobid = sbatch("--dependency=afterok:#{@patch_jobid} #{ionpeel_filename}").match(/Submitted batch job (\d+)/)[1].to_i if @type=="EOR2"
            ##ionpeel_filename = "q_ionpeel_off_moon.sh" if @type == "moon" 
            ##@patch_jobid = sbatch("--dependency=afterok:#{@patch_jobid} #{ionpeel_filename}").match(/Submitted batch job (\d+)/)[1].to_i 
            #export_uvfits_filename = "q_export_uvfits_0.sh" 
            #@patch_jobid = sbatch("--dependency=afterok:#{@patch_jobid} #{export_uvfits_filename}").match(/Submitted batch job (\d+)/)[1].to_i if @type=="EOR2"
            #image_filename = "q_image_moon.sh" unless @type == "moon"
            #image_filename = "q_image_on_moon.sh" if @type == "moon"
            #@patch_jobid = sbatch("--dependency=afterok:#{@patch_jobid} #{image_filename}").match(/Submitted batch job (\d+)/)[1].to_i if @type=="moon"
            #image_filename = "q_image_off_moon.sh" if @type == "moon"
            #@patch_jobid = sbatch("--dependency=afterok:#{@patch_jobid} #{image_filename}").match(/Submitted batch job (\d+)/)[1].to_i if @type=="moon"
            #pbcorr_filename = "q_pbcorr_moon.sh" unless @type == "moon"
            #pbcorr_filename = "q_pbcorr_on_moon.sh" if @type == "moon"            
            #@patch_jobid = sbatch("--dependency=afterok:#{@patch_jobid} #{pbcorr_filename}").match(/Submitted batch job (\d+)/)[1].to_i if @type=="moon"
            #pbcorr_filename = "q_pbcorr_off_moon.sh" if @type == "moon" 
            #@patch_jobid = sbatch("--dependency=afterok:#{@patch_jobid} #{pbcorr_filename}").match(/Submitted batch job (\d+)/)[1].to_i if @type=="moon"
        end
    end

    def rts_peel(mins: 30)
        num_nodes = @subband_ids.split(',').length + 1

        filename = "rts_peel.sh"
        contents = generate_slurm_header(job_name: "pe_#{@obsid}",
                                         machine: "galaxy",
                                         partition: "gpuq",
                                         mins: mins,
                                         nodes: num_nodes,
                                         output: "RTS-peel-#{@obsid}-%A.out")
        contents << "
srun -n #{num_nodes} #{@rts_path} #{ENV["USER"]}_rts_1.in
"

        Dir.chdir "#{@path}/#{@timestamp_dir}" unless Dir.pwd == "#{@path}/#{@timestamp_dir}"
        write(file: filename, contents: contents)
        @peel_jobid = sbatch("--dependency=afterok:#{@patch_jobid} #{filename}").match(/Submitted batch job (\d+)/)[1].to_i
    end

    def rts_status
        @status, @final_message = check_rts_status(path: "#{@path}/#{@timestamp_dir}")
    end
end
