require "fileutils"

abort("$MWA_DIR not defined.") unless ENV["MWA_DIR"]
$mwa_dir = ENV["MWA_DIR"].chomp('/')

def get_queue(machine, user)
    queue = `squeue -M #{machine} -u #{user} 2>&1`
    # Sometimes the queue system needs a little time to think.
    while queue =~ /error/
        STDERR.puts "Slurm error, waiting 10s"
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

def integration_time(path: '.')
    metafits = Dir.glob("#{path}/*_metafits_ppds.fits").first
    abort("#{Dir.pwd}: metafits file not found!") unless metafits
    `python -c "from astropy.io import fits; print(fits.open('#{metafits}')[0].header['INTTIME'])" 2>&1`.split.last
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

def rts_version(path)
    git_dir = path.split("/bin")[0]
    path << "\n\n" << `git --git-dir #{git_dir}/.git log "HEAD^..HEAD"`
end

def download(obsid, mins: 10)
    contents = "#!/bin/bash

#SBATCH --job-name=dl_#{obsid}
#SBATCH --output=getNGASdata-#{obsid}-%A.out
#SBATCH --ntasks=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=#{mins2hms(mins)}
#SBATCH --clusters=zeus
#SBATCH --partition=copyq
#SBATCH --account=mwaeor
#SBATCH --export=NONE

module load pyephem
module load setuptools
cd #{$mwa_dir}/data
# obsdownload.py -o #{obsid} --chstart=1 --chcount=24
# obsdownload.py -o #{obsid} -f
# obsdownload.py -o #{obsid} -m
obsdownload2.py -o #{obsid} -u
"

    FileUtils.mkdir_p "#{$mwa_dir}/data/#{obsid}"
    Dir.chdir "#{$mwa_dir}/data/#{obsid}"
    write("#{obsid}.sh", contents)
    sbatch("#{obsid}.sh").match(/Submitted batch job (\d+)/)[1].to_i
end

def fix_gpubox_timestamps
    # Fix the "off by one" gpubox files by symlinking the offenders
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

def rts_setup(obsid, mins: 5)
    int_time = integration_time(path: "#{$mwa_dir}/data/#{obsid}")
    # Older data
    if int_time == "0.5"
        corr_dumps_per_cadence_cal = 128
        number_of_integration_bins_cal = 7
        number_of_iterations_cal = 1

        corr_dumps_per_cadence_peel = 16
        number_of_integration_bins_peel = 5
        number_of_iterations_peel = 14
    # Newer data
    elsif int_time == "2.0"
        corr_dumps_per_cadence_cal = 32
        number_of_integration_bins_cal = 6
        number_of_iterations_cal = 1

        corr_dumps_per_cadence_peel = 4
        number_of_integration_bins_peel = 3
        number_of_iterations_peel = 14
    end

    source_catalogue_file = "#{$mwa_dir}/data/#{obsid}/srclist_puma-v2_complete_#{obsid}_patch1000.txt"
    date, time = Time.now.to_s.split[0..1]
    timestamp_dir = [date, time.split(':')[0..1].join].join('_')

    contents = "#!/bin/bash

#SBATCH --job-name=se_#{obsid}
#SBATCH --output=RTS-setup-#{obsid}-%A.out
#SBATCH --ntasks=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=#{mins2hms(mins)}
#SBATCH --partition=gpuq
#SBATCH --account=mwaeor
#SBATCH --export=NONE

module load pyephem
module load setuptools

list_gpubox_files.py obsid.dat
ln -sf ../gpufiles_list.dat .

generate_dynamic_RTS_sourcelists.py -n 1000 \\
                                    --sourcelist=/group/mwaeor/bpindor/PUMA/srclists/srclist_puma-v2_complete.txt \\
                                    --obslist=#{$mwa_dir}/data/#{obsid}/#{timestamp_dir}/obsid.dat

generate_mwac_qRTS_auto.py #{$mwa_dir}/data/#{obsid}/#{timestamp_dir}/obsid.dat \\
                           cj 24 \\
                           /group/mwaeor/bpindor/templates/EOR0_selfCalandPeel_PUMA1000_WriteUV_80khz_cotter_template.dat \\
                           --auto \\
                           --chunk_number=0 \\
                           --channel_flags=/group/mwaeor/bpindor/templates/flagged_channels_default.txt \\
                           --dynamic_sourcelist=1000 \\
                           --sourcelist=/group/mwaeor/bpindor/PUMA/srclists/srclist_puma-v2_complete.txt

reflag_mwaf_files.py #{$mwa_dir}/data/#{obsid}/#{timestamp_dir}/obsid.dat

generate_RTS_in_mwac.py #{$mwa_dir}/data/#{obsid} \\
                        cj 24 128T \\
                        --templates=/group/mwaeor/bpindor/templates/EOR0_selfCalandPeel_PUMA1000_WriteUV_80khz_cotter_template.dat \\
                        --header=#{obsid}_metafits_ppds.fits \\
                        --channel_flags=/group/mwaeor/bpindor/templates/flagged_channels_default.txt

mv cj_rts_0.in #{ENV["USER"]}_rts_0.in
mv cj_rts_1.in #{ENV["USER"]}_rts_1.in

sed -i \"s|\\(CorrDumpsPerCadence=\\).*|\\1#{corr_dumps_per_cadence_cal}|\" #{ENV["USER"]}_rts_0.in
sed -i \"s|\\(NumberOfIntegrationBins=\\).*|\\1#{number_of_integration_bins_cal}|\" #{ENV["USER"]}_rts_0.in
sed -i \"s|\\(NumberOfIterations=\\).*|\\1#{number_of_iterations_cal}|\" #{ENV["USER"]}_rts_0.in
sed -i \"s|//SourceCatalogueFile.*||; s|\\(SourceCatalogueFile=\\).*|\\1#{source_catalogue_file}|\" #{ENV["USER"]}_rts_0.in
sed -i \"s|\\(doRFIflagging=\\).*|\\11|\" #{ENV["USER"]}_rts_0.in

sed -i \"s|\\(CorrDumpsPerCadence=\\).*|\\1#{corr_dumps_per_cadence_peel}|\" #{ENV["USER"]}_rts_1.in
sed -i \"s|\\(NumberOfIntegrationBins=\\).*|\\1#{number_of_integration_bins_peel}|\" #{ENV["USER"]}_rts_1.in
sed -i \"s|\\(NumberOfIterations=\\).*|\\1#{number_of_iterations_peel}|\" #{ENV["USER"]}_rts_1.in
sed -i \"s|\\(doRFIflagging=\\).*|\\11|\" #{ENV["USER"]}_rts_1.in
"

    Dir.chdir "#{$mwa_dir}/data/#{obsid}"
    fix_gpubox_timestamps
    FileUtils.mkdir_p "#{$mwa_dir}/data/#{obsid}/#{timestamp_dir}"
    Dir.chdir "#{$mwa_dir}/data/#{obsid}/#{timestamp_dir}"
    system("ln -sf ../*metafits_ppds.fits .")
    write("obsid.dat", obsid)
    write("rts_setup.sh", contents)
    jobid = sbatch("rts_setup.sh").match(/Submitted batch job (\d+)/)[1].to_i
    return jobid, timestamp_dir
end

def rts_patch(obsid, dependent_jobid, timestamp_dir, mins: 15, peel: false, rts_path: "/group/mwaeor/CODE/RTS/bin/rts_gpu")
    filename = "rts_patch.sh"
    contents = "#!/bin/bash

#SBATCH --job-name=pa_#{obsid}
#SBATCH --output=RTS-patch-#{obsid}-%A.out
#SBATCH --nodes=25
#SBATCH --ntasks-per-node=1
#SBATCH --time=#{mins2hms(mins)}
#SBATCH --partition=gpuq
#SBATCH --account=mwaeor
#SBATCH --export=NONE

aprun -n 25 -N 1 #{rts_path} #{ENV["USER"]}_rts_0.in
/group/mwaeor/cjordan/Software/plot_BPcal_128T.py
"

    Dir.chdir "#{$mwa_dir}/data/#{obsid}/#{timestamp_dir}"
    contents << "aprun -n 25 -N 1 #{rts_path} #{ENV["USER"]}_rts_1.in\n" if peel

    write(filename, contents)
    write("rts_version_used.txt", rts_version(rts_path))
    sbatch("--dependency=afterok:#{dependent_jobid} #{filename}").match(/Submitted batch job (\d+)/)[1].to_i
end

def flag_tiles
    # A cheap, horrible hack until I can be bothered doing something more appropriate.
    # For now, just look at the bandpass calibration solutions.
    bp_output = `/group/mwaeor/cjordan/Software/plot_BPcal_128T.py`
    bp_output.scan(/\(flag\s+(\d+)\?\)/).flatten.uniq.join("\n")
end

def rts_peel(obsid, dependent_jobid, timestamp_dir, mins: 30, rts_path: "/group/mwaeor/CODE/RTS/bin/rts_gpu")
    filename = "rts_peel.sh"
    contents = "#!/bin/bash

#SBATCH --job-name=pe_#{obsid}
#SBATCH --output=RTS-peel-#{obsid}-%A.out
#SBATCH --nodes=25
#SBATCH --ntasks-per-node=1
#SBATCH --time=#{mins2hms(mins)}
#SBATCH --partition=gpuq
#SBATCH --account=mwaeor
#SBATCH --export=NONE

aprun -n 25 -N 1 #{rts_path} #{ENV["USER"]}_rts_1.in
"

    Dir.chdir "#{$mwa_dir}/data/#{obsid}/#{timestamp_dir}"
    write(filename, contents)
    sbatch("--dependency=afterok:#{dependent_jobid} #{filename}").match(/Submitted batch job (\d+)/)[1].to_i
end

def rts_status(obsid, timestamp_dir: nil)
    if timestamp_dir
        rts_stdout_log = Dir.glob("#{$mwa_dir}/data/#{obsid}/#{timestamp_dir}/RTS*.out").sort_by { |l| File.mtime(l) }.last
    else
        rts_stdout_log = Dir.glob("#{$mwa_dir}/data/#{obsid}/RTS*.out").sort_by { |l| File.mtime(l) }.last
    end

    # If it's too big, then it failed.
    if File.stat(rts_stdout_log).size.to_f > 1000000
        status = "failed"
        final = "*** huge stdout - tiles probably need to be flagged."
    # If there's a line about "could not open...", the data isn't there.
    elsif File.read(rts_stdout_log).match(/Could not open array file for reading/)
        status = "no data"
        final = "*** probably no gpubox files available for this obsid."
    elsif File.read(rts_stdout_log).match(/Error: Unable to set weighted average frequency. No unflagged channels/)
        status = "???"
        final = "Error: Unable to set weighted average frequency. No unflagged channels"
    end

    # Skip to the end if we already have a status.
    unless status
        # Read the latest node001.log file.
        if timestamp_dir
            node001_log = Dir.glob("#{$mwa_dir}/data/#{obsid}/#{timestamp_dir}/*node001.log").sort_by { |l| File.mtime(l) }.last
        else
            node001_log = Dir.glob("#{$mwa_dir}/data/#{obsid}/*node001.log").sort_by { |l| File.mtime(l) }.last
        end
        if not node001_log
            status = "???"
            final = "*** no node001 log"
        else
            # Read the last line of the log.
            final = File.readlines(node001_log).last.strip
            if final =~ /LogDone. Closing file/
                # Check the file size. Big enough -> peeled. Too small -> patched.
                if File.stat(node001_log).size > 10000000
                    status = "peeled"
                else
                    status = "patched"
                end
            else
                status = "failed"
            end
        end
    end

    return status, final
end
