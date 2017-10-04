#!/usr/bin/env ruby

$LOAD_PATH.unshift "#{File.dirname(__FILE__)}/.."
require "gator"
require "sqlite3"
require "optparse"

OptionParser.new do |opts|
    opts.banner = "Usage: #{__FILE__} [-d /path/to/database]\nDownload all of the obsids specified in the database."
    opts.on("-h", "--help", "Display this message.") {puts opts; exit}

    $database = "#{ENV["MYGROUP"]}/rts.sqlite"
	opts.on("-d", "--database DATABASE", "Specify the database to be used. Default: #{$database}") {|o| $database = o}

    $peel_number = 1000
	opts.on("-p", "--peel_number NUM", "Peel this many sources with the RTS. Default: #{$peel_number}") {|o| $peel_number = o.to_i}
end.parse!

abort("$MWA_DIR not defined.") unless ENV["MWA_DIR"]
abort("$MYGROUP not defined.") unless ENV["MYGROUP"]
abort("$USER not defined.") unless ENV["USER"]


table_name = "downloads"
max_queue_length = 30


begin
    if File.exists?($database)
        db = SQLite3::Database.open $database
    else
        db = SQLite3::Database.new $database
        # db.execute "CREATE TABLE #{table_name}(Obsid INTEGER PRIMARY KEY, Status TEXT, LastChecked TEXT, SetupJobID INTEGER, PatchJobID INTEGER, PeelJobID INTEGER, Timestamp TEXT, Stdout TEXT)"
        db.execute "CREATE TABLE #{table_name}(Obsid INTEGER PRIMARY KEY, Status TEXT, LastChecked TEXT, SetupJobID INTEGER, RTSJobID INTEGER, Timestamp TEXT, Stdout TEXT)"
    end
    db.results_as_hash = true

    # Add specified obsids.
    # Some intelligent code here could determine if the obsid has already been processed.
    obsids_in_table = db.execute("SELECT * FROM #{table_name}").map {|r| r[0]}
    obtain_obsids(ARGV).each do |o|
        next if obsids_in_table.include? o
        obsids_in_table.push o
        db.execute "INSERT INTO #{table_name} VALUES(#{o}, 'unqueued', '#{Time.now}', 0, 0, ' ', ' ')"
    end

    loop do
        _, jobs_in_queue, len_queue = get_queue("galaxy", "cjordan")

        db.execute("SELECT * FROM #{table_name}") do |r|
            obsid = r["Obsid"]
            status = r["Status"]
            status_update = false

            # If this obsid is unqueued, don't do anything yet.
            if status == "unqueued"
            # If this obsid has been peeled or has failed, advance.
            elsif status == "peeled" or status == "failed" or status == "no data" or status == "???"
                next
            elsif jobs_in_queue.include? r["SetupJobID"] and status != "setting up"
                new_status = "setting up"
            elsif jobs_in_queue.include? r["RTSJobID"] and status != "rts running"
                new_status = "rts running"
            elsif not jobs_in_queue.include? r["RTSJobID"]
                new_status, final = rts_status(obsid, timestamp_dir: r["Timestamp"])
                db.execute("UPDATE #{table_name} SET Stdout = '#{final}' WHERE Obsid = #{obsid}")
            end
            if new_status
                if final
                    puts "#{obsid}: #{new_status} - #{final}"
                else
                    puts "#{obsid}: #{new_status}"
                end
                db.execute("UPDATE #{table_name}
                            SET Status = '#{new_status}',
                                LastChecked = '#{Time.now}'
                            WHERE Obsid = #{obsid}")
            end

            # We can't do anything further if we're already at the maximum queue length.
            break if len_queue >= max_queue_length

            if status == "unqueued"
                setup_jobid, timestamp = rts_setup(obsid, peel_number: $peel_number)
                rts_jobid = rts_patch(obsid, setup_jobid, timestamp, mins: 40, peel: true, rts_path: "/group/mwaeor/CODE/RTS/bin/rts_gpu")
                puts "Submitted #{obsid} as jobs #{setup_jobid} and #{rts_jobid}."
                db.execute("UPDATE #{table_name}
                            SET Status = 'setting up',
                                SetupJobID = #{setup_jobid},
                                RTSJobID = #{rts_jobid},
                                LastChecked = '#{Time.now}',
                                Timestamp = '#{timestamp}'
                            WHERE Obsid = #{obsid}")
                len_queue += 3
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
            puts "\nJobs done."
            exit
        else
            puts "Number of obsids not yet processed: #{incomplete}"
        end

        puts "Sleeping...\n\n"
        sleep 60
    end

rescue SQLite3::Exception => e 
    puts "#{e.class}: #{e}"

ensure
    db.close if db
end
