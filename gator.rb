require "fileutils"

abort("$MWA_DIR not defined.") unless ENV["MWA_DIR"]
$mwa_dir = ENV["MWA_DIR"].chomp('/')
$project = ENV["PAWSEY_PROJECT"]

class Float
    def close_to?(n, tol=0.1)
        (self - n).abs < tol
    end
end


def get_queue(machine, user)
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

def write(file, contents)
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

def read_fits_key(fits, key)
    match = File.open(fits, 'r') { |f| f.read(10000).match(/#{key}\s*=\s*(\S+)/) }
    begin
        return match[1]
    rescue
        STDERR.puts "#{fits} does not have a #{key} key!"
        exit 1
    end
end

def alter_config(text, key, value)
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

def generate_slurm_header(job_name, machine, partition, mins)
    return \
"#!/bin/bash
#SBATCH --job-name=#{job_name}
#SBATCH --output=#{job_name}-%A.out
#SBATCH --ntasks=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=#{mins2hms(mins)}
#SBATCH --clusters=#{machine}
#SBATCH --partition=#{partition}
#SBATCH --account=#{$project}
#SBATCH --export=NONE
"
end

def rts_version(path)
    git_dir = path.split("/bin")[0]
    path << "\n\n" << `git --git-dir #{git_dir}/.git log "HEAD^..HEAD"`
end

def flag_tiles
    # A cheap, horrible hack until I can be bothered doing something more appropriate.
    # For now, just look at the bandpass calibration solutions.
    bp_output = `/group/mwaeor/cjordan/Software/plot_BPcal_128T.py`
    bp_output.scan(/\(flag\s+(\d+)\?\)/).flatten.uniq.join("\n")
end

class Obsid
    attr_reader :obsid,
                :type,
                :path,
                :setup_jobid,
                :patch_jobid,
                :peel_jobid,
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
    end

    def fix_gpubox_timestamps
        # Fix the "off by one" gpubox files by symlinking the offenders
        Dir.chdir @path unless Dir.pwd == @path
        gpubox_files = Dir.glob("*gpubox*")
        odd = gpubox_files.select { |f| f.to_i.odd? }
        return if odd.empty?
        even = gpubox_files.select { |f| f.to_i.even? }
        return if even.empty?

        proper_timestamp = gpubox_files.select { |f| f.to_i.even? }.first.split('_')[1]
        odd.each do |f|
            a, _, *c = f.split('_')
            system("ln -sf #{f} #{[a, proper_timestamp, c].join('_')}")
        end
    end

    def integration_time
        metafits = Dir.glob("#{@path}/*metafits*").sort_by { |f| File.size(f) }.last
        abort("#{@obsid}: metafits file not found!") unless metafits
        @int_time = read_fits_key(metafits, "INTTIME")
    end

    def obs_type
        metafits = Dir.glob("#{@path}/*metafits*").sort_by { |f| File.size(f) }.last
        abort("#{@obsid}: metafits file not found!") unless metafits

        # Use the RAPHASE header tag wherever available.
        # EoR-specific fields.
        if read_fits_key(metafits, "PROJECT").include? "G0009" or
          read_fits_key(metafits, "GRIDNAME").include? "EOR" or
          read_fits_key(metafits, "PROJECT").include? "D0000"
            ra = read_fits_key(metafits, "RAPHASE").to_f
            if ra.close_to?(0, tol=3)
                @type = "EOR0"
            elsif ra.close_to?(60, tol=3)
                @type = "EOR1"
            elsif ra.close_to?(30, tol=5)
                @type = "LymanA"
            else
                @type = "RA=#{ra}"
            end
        else
            @type = "RA=#{ra}"
        end
    end

    def download(mins: 30)
        contents = generate_slurm_header("dl_#{@obsid}", "zeus", "copyq", mins)
        contents << "
module load pyephem
module load setuptools

cd #{$mwa_dir}/data
# obsdownload.py -o #{@obsid} --chstart=1 --chcount=24
# obsdownload.py -o #{@obsid} -f
# obsdownload.py -o #{@obsid} -m
obsdownload2.py -o #{@obsid} -u
"

        FileUtils.mkdir_p @path unless Dir.pwd == @path
        Dir.chdir @path unless Dir.pwd == @path
        write("#{@obsid}.sh", contents)
        sbatch("#{@obsid}.sh").match(/Submitted batch job (\d+)/)[1].to_i
    end

    def rts(setup_mins: 5, cal_mins: 40, patch: true, peel: true, peel_number: 1000, timestamp: true, rts_path: "/group/mwaeor/CODE/RTS/bin/rts_gpu")
        integration_time unless @int_time
        @peel_number = peel_number
        @rts_path = rts_path

        # Older data
        if @int_time == "0.5"
            @corr_dumps_per_cadence_cal = 128
            @number_of_integration_bins_cal = 7

            @corr_dumps_per_cadence_peel = 16
            @number_of_integration_bins_peel = 5
        # Newer data
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

        obs_type unless @type
        if @type == "EOR0"
            @obs_image_centre_ra = "0."
            @obs_image_centre_dec = "-27.0"
            @source_list = "/group/mwaeor/bpindor/PUMA/srclists/srclist_puma-v2_complete.txt"
            @patch_source_catalogue_file = "#{$mwa_dir}/data/#{@obsid}/srclist_puma-v2_complete_#{@obsid}_patch1000.txt"
            @peel_source_catalogue_file = "/group/mwaeor/bpindor/PUMA/srclists/srclist_puma-v2_complete_1061316296_peel3000.txt"
            @subband_ids = (1..24).to_a.join(',')

            rts_setup(mins: setup_mins)
            rts_patch(mins: cal_mins, peel: peel) if patch
        elsif @type == "EOR1"
            @obs_image_centre_ra = "4.0"
            @obs_image_centre_dec = "-30.0"
            @source_list = "/group/mwaeor/bpindor/PUMA/srclists/srclist_pumaIDR4_EoR1-ext-only+ForA-shap.txt"
            @patch_source_catalogue_file = "#{$mwa_dir}/data/#{@obsid}/srclist_pumaIDR4_EoR1-ext-only+ForA-shap_#{@obsid}_patch1000.txt"
            @peel_source_catalogue_file = "/group/mwaeor/bpindor/PUMA/srclists/srclist_pumaIDR4_EoR1-ext-only+ForA-shap_1062364544_peel3000.txt"
            @subband_ids = (1..24).to_a.join(',')

            rts_setup(mins: setup_mins)
            rts_patch(mins: cal_mins, peel: peel) if patch
        elsif @type == "LymanA"
            @obs_image_centre_ra = "2.283"
            @obs_image_centre_dec = "-5.0"
            @source_list = "/group/mwaeor/ctrott/srclist_puma-v2_complete_1186437224_patch1000.txt"
            @patch_source_catalogue_file = "#{$mwa_dir}/data/#{@obsid}/srclist_puma-v2_complete_1186437224_patch1000_#{@obsid}_patch1000.txt"
            @peel_source_catalogue_file = "/group/mwaeor/ctrott/srclist_puma-v2_complete_1186437224_peel1000.txt"

            # High band
            # Frequency of channel 24, if all bands were contiguous (identical here, i.e. channel 142)
            # This frequency is actually about half a channel lower in frequency, i.e. 141.49609*1.28MHz
            @obs_freq_base = 181.135
            @subband_ids = (17..24).to_a.join(',')
            @timestamp_dir << "_high"
            rts_setup(mins: setup_mins)
            rts_patch(mins: cal_mins, peel: peel) if patch

            # Low band
            # Frequency of channel 24, if all bands were contiguous (channel 150 here, even though our lowest is 158)
            # This frequency is actually about half a channel lower in frequency, i.e. 149.49609*1.28MHz
            @obs_freq_base = 191.355
            @subband_ids = (1..16).to_a.join(',')
            @timestamp_dir = @timestamp_dir.split('_').select { |e| e =~ /\d/ }.join('_') << "_low"
            rts_setup(mins: setup_mins)
            rts_patch(mins: cal_mins, peel: peel) if patch
        else
            abort(sprintf "Unknown grid name! (%s for %s)", @type, @obsid)
        end
    end

    def rts_setup(mins: 5)
        contents = "#!/bin/bash

#SBATCH --job-name=se_#{@obsid}
#SBATCH --output=RTS-setup-#{@obsid}-%A.out
#SBATCH --ntasks=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=#{mins2hms(mins)}
#SBATCH --partition=gpuq
#SBATCH --account=#{$project}
#SBATCH --export=NONE

module load pyephem
module load setuptools

list_gpubox_files.py obsid.dat
ln -sf ../gpufiles_list.dat .

generate_dynamic_RTS_sourcelists.py -n 1000 \\
                                    --sourcelist=#{@source_list} \\
                                    --obslist=#{@path}/#{@timestamp_dir}/obsid.dat

generate_mwac_qRTS_auto.py #{@path}/#{@timestamp_dir}/obsid.dat \\
                           cj 24 \\
                           /group/mwaeor/bpindor/templates/EOR0_selfCalandPeel_PUMA1000_WriteUV_80khz_cotter_template.dat \\
                           --auto \\
                           --chunk_number=0 \\
                           --channel_flags=/group/mwaeor/bpindor/templates/flagged_channels_default.txt \\
                           --dynamic_sourcelist=1000 \\
                           --sourcelist=#{@source_list}

reflag_mwaf_files.py #{@path}/#{@timestamp_dir}/obsid.dat

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
        fix_gpubox_timestamps
        FileUtils.mkdir_p @timestamp_dir
        Dir.chdir @timestamp_dir
        system("ln -sf ../*metafits_ppds.fits .")
        write("obsid.dat", @obsid)
        write("rts_setup.sh", contents)
        @setup_jobid = sbatch("rts_setup.sh").match(/Submitted batch job (\d+)/)[1].to_i
    end

    def rts_patch(mins: 15, peel: false)
        num_nodes = @subband_ids.split(',').length + 1

        filename = "rts_patch.sh"
        contents = "#!/bin/bash

#SBATCH --job-name=pa_#{@obsid}
#SBATCH --output=RTS-patch-#{@obsid}-%A.out
#SBATCH --nodes=#{num_nodes}
#SBATCH --ntasks-per-node=1
#SBATCH --time=#{mins2hms(mins)}
#SBATCH --partition=gpuq
#SBATCH --account=#{$project}
#SBATCH --export=NONE

aprun -n #{num_nodes} -N 1 #{@rts_path} #{ENV["USER"]}_rts_0.in
/group/mwaeor/cjordan/Software/plot_BPcal_128T.py
/group/mwaeor/cjordan/Software/plot_CalSols.py --base_dir=`pwd` -n #{@obsid} -i
touch flagged_tiles.txt
aprun -n #{num_nodes} -N 1 #{@rts_path} #{ENV["USER"]}_rts_0.in
"
        contents << "aprun -n #{num_nodes} -N 1 #{@rts_path} #{ENV["USER"]}_rts_1.in\n" if peel

        Dir.chdir "#{@path}/#{@timestamp_dir}" unless Dir.pwd == "#{@path}/#{@timestamp_dir}"
        write(filename, contents)
        write("rts_version_used.txt", rts_version(@rts_path))
        @patch_jobid = sbatch("--dependency=afterok:#{@setup_jobid} #{filename}").match(/Submitted batch job (\d+)/)[1].to_i
    end

    def rts_peel(mins: 30)
        num_nodes = @subband_ids.split(',').length + 1

        filename = "rts_peel.sh"
        contents = "#!/bin/bash

#SBATCH --job-name=pe_#{@obsid}
#SBATCH --output=RTS-peel-#{@obsid}-%A.out
#SBATCH --nodes=25
#SBATCH --ntasks-per-node=1
#SBATCH --time=#{mins2hms(mins)}
#SBATCH --partition=gpuq
#SBATCH --account=#{$project}
#SBATCH --export=NONE

aprun -n #{num_nodes} -N 1 #{@rts_path} #{ENV["USER"]}_rts_1.in
"

        Dir.chdir "#{@path}/#{@timestamp_dir}" unless Dir.pwd == "#{@path}/#{@timestamp_dir}"
        write(filename, contents)
        @peel_jobid = sbatch("--dependency=afterok:#{@patch_jobid} #{filename}").match(/Submitted batch job (\d+)/)[1].to_i
    end

    def rts_status
        @stdout_log = Dir.glob("#{@path}/#{@timestamp_dir}/RTS*.out").sort_by { |l| File.mtime(l) }.last

        # If there's no log, then maybe the job didn't run.
        if not @stdout_log
            @status = "???"
            @final = "*** no node001 log"
        # If it's too big, then it failed.
        elsif File.stat(@stdout_log).size.to_f > 1000000
            @status = "failed"
            @final = "*** huge stdout - tiles probably need to be flagged."
        # If there's a line about "could not open...", the data isn't there.
        elsif File.read(@stdout_log).match(/Could not open array file for reading/)
            @status = "no data"
            @final = "*** probably no gpubox files available for this obsid."
        elsif File.read(@stdout_log).match(/Error: Unable to set weighted average frequency. No unflagged channels/)
            @status = "???"
            @final = "Error: Unable to set weighted average frequency. No unflagged channels"
        end

        # Skip to the end if we already have a status.
        unless @status
            # Read the latest node001.log file.
            @node001_log = Dir.glob("#{@path}/#{@timestamp_dir}/*node001.log").sort_by { |l| File.mtime(l) }.last
            if not @node001_log
                @status = "???"
                @final = "*** no node001 log"
            else
                # Read the last line of the log.
                @final = File.readlines(node001_log).last.strip
                if @final =~ /LogDone. Closing file/
                    # Check the file size. Big enough -> peeled. Too small -> patched.
                    if File.stat(@node001_log).size > 10000000
                        @status = "peeled"
                    else
                        @status = "patched"
                    end
                else
                    @status = "failed"
                end
            end
        end
    end
end
