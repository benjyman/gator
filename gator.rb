require "fileutils"

abort("$MWA_DIR not defined.") unless ENV["MWA_DIR"]
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
    return \
"#!/bin/bash
#SBATCH --job-name=#{job_name}
#SBATCH --output=#{stdout}
#SBATCH --nodes=#{nodes}
#SBATCH --ntasks-per-node=#{ntasks_per_node}
#SBATCH --time=#{mins2hms(mins)}
#SBATCH --clusters=#{machine}
#SBATCH --partition=#{partition}
#SBATCH --account=#{$project}
#SBATCH --export=NONE
"
end

def rts_version(path)
    git_dir = path.split("/bin")[0]
    git_commit = `git --git-dir #{git_dir}/.git log "HEAD^..HEAD"`
    "#{path}\n\n#{git_commit}"
end

def flag_tiles
    # A cheap, horrible hack until I can be bothered doing something more appropriate.
    # For now, just look at the bandpass calibration solutions.
    bp_output = `/group/mwaeor/cjordan/Software/plot_BPcal_128T.py`
    bp_output.scan(/\(flag\s+(\d+)\?\)/).flatten.uniq.join("\n")
end

def check_rts_status(path: ".")
    stdout_log = Dir.glob("#{path}/RTS*.out").sort_by { |l| File.mtime(l) }.last

    # If there's no log, then maybe the job didn't run.
    if not stdout_log
        status = "???"
        final = "*** no logs"
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
            status = "???"
            final = "*** no node logs"
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
                :node001_log

    def initialize(obsid, path: nil)
        # "obsid" is (probably) a 10 digit string, representing the GPS timestamp
        # of an observation collected with the MWA.
        @obsid = obsid
        if path
            @path = path
        else
            @path = "#{$mwa_dir}/data/#{obsid}"
        end

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
            elsif filename.include? "LymanA"
                @type = "LymanA"
            else
                @type = "RA=#{ra}"
            end
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

    def rts(setup_mins: 5, cal_mins: 40, patch: true, peel: true, peel_number: 1000, timestamp: true, rts_path: "rts_gpu")
        if peel and not patch
            abort "Cannot peel if we are not patching; exiting."
        end
        @patch = patch
        @peel = peel

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

        # Depending on your project, we point to a specific master source list file.
        if $project == "mwasci"
            @source_list = "/group/mwasci/gdrouart/Softwares/srclists/srclist_puma-v3_complete.txt"
        elsif $project == "mwaeor"
            @source_list = "/group/mwaeor/cjordan/srclist_pumav3_EoR0aegean_EoR1pietro+ForA.txt"
        end
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
            # This is for all other "non-special" fields, including EoR fields.
            @subband_ids = (1..24).to_a.join(',')
            rts_setup(mins: setup_mins)
            rts_patch(mins: cal_mins, peel: @peel) if @patch
        end
    end

    def rts_setup(mins: 10)
        contents = generate_slurm_header(job_name: "se_#{@obsid}",
                                         machine: "galaxy",
                                         partition: "gpuq",
                                         mins: mins,
                                         nodes: 1,
                                         output: "RTS-setup-#{@obsid}-%A.out")
        contents << "
list_gpubox_files.py obsid.dat
ln -sf ../gpufiles_list.dat .

#####
# new way / getting rid off the wrapper generate_dynamic_RTS_sourcelists.py
#
# two commands feeding directly srclist_by_beam.py to pick randomly sources and attenuate them 
# by the beam. one to feed the patch (calibrate data) and one to feed in peel (peel sources from data) 
#####
echo \"\nRunning srclist_by_beam.py for a patch source list.\"
srclist_by_beam.py -n 1000 \\
                   --srclist=#{@source_list} \\
                   --metafits=#{@metafits} \\
                   --order=\"distance\"
"
        contents << "
echo \"\nRunning srclist_by_beam.py for a peel source list.\"
srclist_by_beam.py -n 3000 \\
                   --srclist=#{@source_list} \\
                   --metafits=#{@metafits} \\
                   --order=\"distance\" \\
                   --no_patch \\
                   --cutoff=30
" if @peel
        contents << "
#####

#####
# generate the rts_patch.sh file - to many options and stuff. should be more like rts_setup! 
echo \"\nRunning generate_mwac_qRTS_auto.py\"
generate_mwac_qRTS_auto.py #{@path}/#{@timestamp_dir}/obsid.dat \\
                           #{ENV["USER"]} 24 \\
                           /group/mwaeor/bpindor/templates/EOR0_selfCalandPeel_PUMA1000_WriteUV_80khz_cotter_template.dat \\
                           --auto \\
                           --chunk_number=0 \\
                           --dynamic_sourcelist=1000 \\
                           --sourcelist=#{@source_list}
#####

echo \"\nRunning reflag_mwaf_files.py\"
reflag_mwaf_files.py #{@path}/#{@timestamp_dir}/obsid.dat

##### 
# the following should be replaced by a template generation function from metafits header.
echo \"\nRunning generate_RTS_in_mwac.py\"
generate_RTS_in_mwac.py #{@path} \\
                        #{ENV["USER"]} 24 128T \\
                        --templates=/group/mwaeor/bpindor/templates/EOR0_selfCalandPeel_PUMA1000_WriteUV_80khz_cotter_template.dat \\
                        --header=#{@obsid}_metafits_ppds.fits \\
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
"
        contents << "
sed -i \"s|\\(ObservationFrequencyBase=\\).*|\\1#{@obs_freq_base}|\" #{ENV["USER"]}_rts_0.in
sed -i \"s|\\(ObservationFrequencyBase=\\).*|\\1#{@obs_freq_base}|\" #{ENV["USER"]}_rts_1.in
" if @obs_freq_base

        Dir.chdir @path unless Dir.pwd == @path
        FileUtils.mkdir_p @timestamp_dir
        Dir.chdir @timestamp_dir
        system("ln -sf ../*metafits_ppds.fits .")
        write(file: "obsid.dat", contents: @obsid)
        write(file: "rts_setup.sh", contents: contents)
        @setup_jobid = sbatch("rts_setup.sh").match(/Submitted batch job (\d+)/)[1].to_i
    end

    def rts_patch(mins: 15, peel: false)
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
echo \"\nRunning RTS patch\"
aprun -n #{num_nodes} -N 1 #{@rts_path} #{ENV["USER"]}_rts_0.in

echo \"\nRunning plot_BPcal_128T.py\"
/group/mwaeor/cjordan/Software/plot_BPcal_128T.py

echo \"\nRunning plot_CalSols.py\"
/group/mwaeor/cjordan/Software/plot_CalSols.py --base_dir=`pwd` -n #{@obsid} -i
touch flagged_tiles.txt

echo \"\nRunning RTS patch\"
aprun -n #{num_nodes} -N 1 #{@rts_path} #{ENV["USER"]}_rts_0.in
"
        if peel
            contents << "echo \"\nRunning RTS peel\"
aprun -n #{num_nodes} -N 1 #{@rts_path} #{ENV["USER"]}_rts_1.in\n"
        end

        Dir.chdir "#{@path}/#{@timestamp_dir}" unless Dir.pwd == "#{@path}/#{@timestamp_dir}"
        write(file: filename, contents: contents)
        write(file: "rts_version_used.txt", contents: rts_version(@rts_path))
        @patch_jobid = sbatch("--dependency=afterok:#{@setup_jobid} #{filename}").match(/Submitted batch job (\d+)/)[1].to_i
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
aprun -n #{num_nodes} -N 1 #{@rts_path} #{ENV["USER"]}_rts_1.in
"

        Dir.chdir "#{@path}/#{@timestamp_dir}" unless Dir.pwd == "#{@path}/#{@timestamp_dir}"
        write(file: filename, contents: contents)
        @peel_jobid = sbatch("--dependency=afterok:#{@patch_jobid} #{filename}").match(/Submitted batch job (\d+)/)[1].to_i
    end

    def rts_status
        check_rts_status(path: "#{@path}/#{@timestamp_dir}")
    end
end
