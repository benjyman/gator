#!/usr/bin/env ruby

require "time"
require "sqlite3"
require "optparse"

$LOAD_PATH.unshift "#{File.dirname(__FILE__)}/.."
require "gator"

OptionParser.new do |opts|
    opts.banner = "Usage: #{__FILE__} [-d /path/to/database]\nDownload all of the obsids specified in the database."
    opts.on("-h", "--help", "Display this message.") {puts opts; exit}

    $database = "#{ENV["MYGROUP"]}/rts.sqlite"
	opts.on("-d", "--database DATABASE", "Specify the database to be used. Default: #{$database}") {|o| $database = o}

    $peel_number = 1000
	opts.on("-p", "--peel_number NUM", "Peel this many sources with the RTS. Default: #{$peel_number}") {|o| $peel_number = o.to_i}

    $run_time = 40
	opts.on("-t", "--run_time NUM", "Allow the RTS to run for this many minutes. Default: #{$run_time}") {|o| $run_time = o.to_i}

    $sleep_time = 60
	opts.on("-s", "--sleep_time TIME", "Amount of time to wait before re-analysing the queue (seconds). Default: #{$sleep_time}") {|o| $sleep_time = o.to_i}

    $timestamp = true
	opts.on("--timestamp", "Disable the usage of a timestamp directory for this RTS run. Default: #{$timestamp}") {$timestamp = false}

    $patch = true
	opts.on("--patch", "Disable the patch step. Default: #{$patch}") {$patch = false}

    $peel = true
	opts.on("--peel", "Disable the peel step. Default: #{$peel}") {$peel = false}

    $srclist = "#{ENV["SRCLIST_ROOT"]}/srclist_pumav3_EoR0aegean_EoR1pietro+ForA.txt"
	opts.on("--srclist", "Specify the source list to be used. Default: #{$srclist}") {|o| $srclist = o}

    $rts_path = `which rts_gpu`.strip
	opts.on("--rts_path", "Specify the path to the RTS executable. Default: #{$rts_path}") {|o| $rts_path = o}
end.parse!

abort("$MWA_DIR not defined.") unless ENV["MWA_DIR"]
abort("$MYGROUP not defined.") unless ENV["MYGROUP"]
abort("$USER not defined.") unless ENV["USER"]


table_name = "downloads"
max_queue_length = 12


abort("Database (#{$database}) doesn't exist!") unless File.exists?($database)
db = SQLite3::Database.open $database
db.results_as_hash = true

begin
    # Loop forever, until all obsids have been processed.
    loop do
        _, jobs_in_queue, len_queue = get_queue(machine: "galaxy", user: ENV["USER"])

        db.execute("SELECT * FROM #{table_name}") do |r|
            obsid = r["Obsid"]
            path = r["Path"]
            status = r["Status"]
            new_status = nil

            # If this obsid is unqueued, don't do anything yet.
            if status == "unqueued"
            # If this obsid has been peeled or has failed, advance.
            elsif status == "peeled" or status == "failed" or status == "no data" or status == "???"
                next
            elsif jobs_in_queue.include? r["SetupJobID"] and status != "setting up"
                new_status = "setting up"
            elsif jobs_in_queue.include? r["RTSJobID"] and status != "rts running"
                new_status = "rts running"
            elsif not jobs_in_queue.include? r["RTSJobID"] and Time.now - Time.parse(r["LastChecked"]) > 30
                new_status, final = check_rts_status(path: path)
                db.execute("UPDATE #{table_name}
                            SET Stdout = '#{final}'
                            WHERE Obsid = #{obsid} AND Path = '#{path}'")
            end
            if new_status
                if final
                    if new_status == "peeled"
                        puts "#{obsid.to_s.green}: #{new_status} - #{final}"
                    else
                        puts "#{obsid.to_s.red}: #{new_status} - #{final}"
                    end
                else
                    puts "#{obsid.to_s.blue}: #{new_status}"
                end
                if new_status == "peeled"
                    # Upload the successful RTS results to the QA database.
                    iono_mag, iono_pca, iono_qa = determine_iono_metrics(obsid: obsid,
                                                                         path: path,
                                                                         srclist: $srclist)

                    # Update the SQLite database.
                    db.execute("UPDATE #{table_name}
                                SET Status = '#{new_status}',
                                    LastChecked = '#{Time.now}',
                                    IonoMagnitude = '#{iono_mag}',
                                    IonoPCA = '#{iono_pca}',
                                    IonoQA = '#{iono_qa}'
                                WHERE Obsid = #{obsid} AND Path = '#{path}'")
                else
                    # If the new status isn't "peeled", then simply update the SQLite database.
                    db.execute("UPDATE #{table_name}
                                SET Status = '#{new_status}',
                                    LastChecked = '#{Time.now}'
                                WHERE Obsid = #{obsid} AND Path = '#{path}'")
                end
            end

            # We can't do anything further if we're already at the maximum queue length.
            break if len_queue >= max_queue_length

            if status == "unqueued"
                o = Obsid.new(obsid)
                o.rts(setup_mins: 10,
                      cal_mins: $run_time,
                      patch: r["Patch"] == 1 ? true : false,
                      peel: r["Peel"] == 1 ? true : false,
                      peel_number: r["PeelNumber"],
                      timestamp: r["Timestamp"] == 1 ? true : false,
                      srclist: $srclist,
                      rts_path: $rts_path)
                puts "Submitted #{obsid.to_s.blue} as #{o.setup_jobid.to_s.blue} (type: #{o.type})."
                if o.type == "LymanA"
                    low_path = "#{o.path}/#{o.timestamp_dir}"
                    high_path = "#{o.path}/" << o.timestamp_dir.split('_').select { |e| e =~ /\d/ }.join('_') << "_high"
                    db.execute("UPDATE #{table_name}
                                SET Status = 'setting up',
                                    Path = '#{high_path}',
                                    SetupJobID = #{o.high_setup_jobid},
                                    RTSJobID = #{o.high_patch_jobid},
                                    LastChecked = '#{Time.now}'
                                WHERE Obsid = #{obsid} AND Path = 'high'")
                    db.execute("UPDATE #{table_name}
                                SET Status = 'setting up',
                                    Path = '#{low_path}',
                                    SetupJobID = #{o.low_setup_jobid},
                                    RTSJobID = #{o.low_patch_jobid},
                                    LastChecked = '#{Time.now}'
                                WHERE Obsid = #{obsid} AND Path = 'low'")
                    len_queue += 4
                else
                    db.execute("UPDATE #{table_name}
                                SET Status = 'setting up',
                                    Path = '#{o.path}/#{o.timestamp_dir}',
                                    SetupJobID = #{o.setup_jobid},
                                    RTSJobID = #{o.patch_jobid},
                                    LastChecked = '#{Time.now}'
                                WHERE Obsid = #{obsid} AND Path = '#{path}'")
                    len_queue += 2
                end
            end
        end

        # Check the status of all obsids - have they all been completed? If so, exit.
        incomplete = 0
        db.execute("SELECT * FROM #{table_name}") do |r|
            status = r["Status"]
            unless status == "peeled" or status == "failed" or status == "no data" or status == "???"
                incomplete += 1
            end
        end
        if incomplete == 0
            puts "\nJobs done.".green
            exit
        else
            puts "Number of obsids not yet processed: #{incomplete.to_s.yellow}"
        end

        puts "Sleeping...\n\n"
        sleep $sleep_time
    end

rescue SQLite3::Exception => e
    puts "#{e.class}: #{e}"

ensure
    db.close if db
end
