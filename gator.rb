def get_queue(machine, user)
    queue = `squeue -M #{machine} -u #{user}`
    jobs_in_queue = queue.split("\n").map { |l| l.split[0].to_i if l =~ /^\d/ }.compact
    len_queue = jobs_in_queue.length
    return queue, jobs_in_queue, len_queue
end

def mins2hms(mins)
    # The following code uses seconds, so convert from minutes.
    t = (mins*60).to_i
    "%02d:%02d:%02d" % [t/86400*24 + t/3600%24, t/60%60, t%60]
end

def obtain_obsids(argv)
    def check_and_push(obsids, obsid)
        return if obsid.strip.empty? or obsid.to_i == 0
        obsids.push(obsid.to_i)
    end

    obsids = []
    argv.each do |o|
        # Check if this argument is a file - if so, assume it contains obsids.
        if File.exists?(o)
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

def download(obsid, mins=10)
    abort("$MWA_DIR not defined.") unless ENV["MWA_DIR"]

    contents = "#!/bin/bash

#SBATCH --job-name=dwl#{obsid}
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
cd #{ENV["MWA_DIR"]}/data
obsdownload.py -o #{obsid} --chstart=1 --chcount=24
obsdownload.py -o #{obsid} -f
obsdownload.py -o #{obsid} -m
"

    File.open("#{obsid}.sh", 'w') { |f| f.puts contents }
    # `sbatch #{obsid}.sh`.match(/Submitted batch job (\d+)/)[1].to_i
end

def rts_setup(obsid, mins=5)
    abort("$MWA_DIR not defined.") unless ENV["MWA_DIR"]

    filename = "rts_setup.sh"
    contents = "#!/bin/bash

#SBATCH --job-name=stp#{obsid}
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

aprun generate_dynamic_RTS_sourcelists.py \\
      -n 1000 \\
      --sourcelist=/group/mwaeor/bpindor/PUMA/srclists/srclist_puma-v2_complete.txt \\
      --obslist=/scratch2/mwaeor/MWA/data/#{obsid}/obsid.dat

aprun generate_mwac_qRTS_auto.py \\
      /scratch2/mwaeor/MWA/data/#{obsid}/obsid.dat \\
      cj 24 \\
      /group/mwaeor/bpindor/templates/EOR0_selfCalandPeel_PUMA1000_WriteUV_80khz_cotter_template.dat \\
      --auto \\
      --chunk_number=0 \\
      --channel_flags=/group/mwaeor/bpindor/templates/flagged_channels_default.txt \\
      --dynamic_sourcelist=1000 \\
      --sourcelist=/group/mwaeor/bpindor/PUMA/srclists/srclist_puma-v2_complete.txt

aprun reflag_mwaf_files.py \\
      /scratch2/mwaeor/MWA/data/#{obsid}/obsid.dat 
"

    Dir.chdir "#{ENV["MWA_DIR"]}/data/#{obsid}"
    File.open("obsid.dat", 'w') { |f| f.puts obsid }
    File.open(filename, 'w') { |f| f.puts contents }
    `sbatch #{filename}`.match(/Submitted batch job (\d+)/)[1].to_i
end

def rts_patch(obsid, dependent_jobid, mins=10)
    abort("$MWA_DIR not defined.") unless ENV["MWA_DIR"]

    filename = "rts_patch.sh"
    contents = "#!/bin/bash

#SBATCH --job-name=pat#{obsid}
#SBATCH --output=RTS-patch-#{obsid}-%A.out
#SBATCH --nodes=25
#SBATCH --ntasks-per-node=1
#SBATCH --time=#{mins2hms(mins)}
#SBATCH --partition=gpuq
#SBATCH --account=mwaeor
#SBATCH --export=NONE

generate_RTS_in_mwac.py /scratch2/mwaeor/MWA/data/#{obsid} \\
                        cj 24 128T \\
                        --templates=/group/mwaeor/bpindor/templates/EOR0_selfCalandPeel_PUMA1000_WriteUV_80khz_cotter_template.dat \\
                        --header=#{obsid}_metafits_ppds.fits \\
                        --channel_flags=/group/mwaeor/bpindor/templates/flagged_channels_default.txt

sed -i s,/scratch2/mwaeor/bpindor/pp_selfcal/uniq_300conv_eor0.txt,/scratch2/mwaeor/MWA/data/#{obsid}/srclist_puma-v2_complete_#{obsid}_patch1000.txt,g cj_rts_0.in

aprun -n 25 -N 1 rts_gpu cj_rts_0.in
"

    Dir.chdir "#{ENV["MWA_DIR"]}/data/#{obsid}"
    File.open(filename, 'w') { |f| f.puts contents }
    `sbatch --dependency=afterok:#{dependent_jobid} #{filename}`.match(/Submitted batch job (\d+)/)[1].to_i
end

def rts_peel(obsid, dependent_jobid, mins=20)
    abort("$MWA_DIR not defined.") unless ENV["MWA_DIR"]

    filename = "rts_peel.sh"
    contents = "#!/bin/bash

#SBATCH --job-name=pel#{obsid}
#SBATCH --output=RTS-peel-#{obsid}-%A.out
#SBATCH --nodes=25
#SBATCH --ntasks-per-node=1
#SBATCH --time=#{mins2hms(mins)}
#SBATCH --partition=gpuq
#SBATCH --account=mwaeor
#SBATCH --export=NONE

aprun -n 25 -N 1 rts_gpu cj_rts_1.in
"

    Dir.chdir "#{ENV["MWA_DIR"]}/data/#{obsid}"
    File.open(filename, 'w') { |f| f.puts contents }
    `sbatch --dependency=afterok:#{dependent_jobid} #{filename}`.match(/Submitted batch job (\d+)/)[1].to_i
end
