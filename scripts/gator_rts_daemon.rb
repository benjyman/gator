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

    $retry_time = 1800
	opts.on("-r", "--retry_time TIME", "If an obsid download job failed, wait this long before retrying (seconds). Default: #{$retry_time}") {|o| $retry_time = o}
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
        db.execute "CREATE TABLE #{table_name}(Obsid INTEGER PRIMARY KEY, Status TEXT, LastChecked TEXT, SetupJobID INTEGER, PatchJobID INTEGER, PeelJobID INTEGER, Stdout TEXT)"
    end
    db.results_as_hash = true

    # Add specified obsids.
    # Some intelligent code here could determine if the obsid has already been processed.
    obsids_in_table = db.execute("select * from #{table_name}").map {|r| r[0]}
    obtain_obsids(ARGV).each do |o|
        next if obsids_in_table.include? o
        obsids_in_table.push o
        db.execute "INSERT INTO #{table_name} VALUES(#{o}, 'unqueued', '#{Time.now}', 0, 0, 0, ' ')"
    end

    loop do
        _, jobs_in_queue, len_queue = get_queue("galaxy", "cjordan")

        db.execute("select * from #{table_name}") do |r|
            obsid = r["Obsid"]
            status = r["Status"]
            status_update = false

            # If this obsid has been peeled, advance.
            if status == "peeled"
                next
            elsif jobs_in_queue.include? r["SetupJobID"]
                status = "setting up"
                status_update = true
            elsif jobs_in_queue.include? r["PatchJobID"]
                status = "patching"
                status_update = true
            elsif jobs_in_queue.include? r["PeelJobID"]
                status = "peeling"
                status_update = true
            elsif not jobs_in_queue.include? r["PeelJobID"]
                if status == "patching" or status == "peeling"
                    status, final = rts_status(obsid)
                    status_update = true
                    db.execute("UPDATE #{table_name} SET Stdout = '#{final}' WHERE Obsid = #{obsid}")
                end
            end

            if status_update
                db.execute("UPDATE #{table_name} SET Status = '#{status}' WHERE Obsid = #{obsid}")
                db.execute("UPDATE #{table_name} SET LastChecked = '#{Time.now}' WHERE Obsid = #{obsid}")
            end

            # We can't do anything further if we're already at the maximum queue length.
            break if len_queue >= max_queue_length

            if status == "patched" and not jobs_in_queue.include? r["PeelJobID"]
                peel_jobid = rts_peel(obsid, 1)
                db.execute("UPDATE #{table_name} SET PeelJobID = #{peel_jobid} WHERE Obsid = #{obsid}")
            elsif status == "unqueued"
                setup_jobid = rts_setup(obsid)
                patch_jobid = rts_patch(obsid, setup_jobid)
                peel_jobid = rts_peel(obsid, patch_jobid)
                puts "Submitted #{obsid} as jobs #{setup_jobid}, #{patch_jobid}, #{peel_jobid}."
                db.execute("UPDATE #{table_name} SET Status = 'setting up' WHERE Obsid = #{obsid}")
                db.execute("UPDATE #{table_name} SET SetupJobID = #{setup_jobid} WHERE Obsid = #{obsid}")
                db.execute("UPDATE #{table_name} SET PatchJobID = #{patch_jobid} WHERE Obsid = #{obsid}")
                db.execute("UPDATE #{table_name} SET PeelJobID = #{peel_jobid} WHERE Obsid = #{obsid}")
                db.execute("UPDATE #{table_name} SET LastChecked = '#{Time.now}' WHERE Obsid = #{obsid}")
                len_queue += 3
            end
        end

        # # Check the status of all obsids - have they all been peeled? If so, exit.
        # num_peeled = db.execute("select * from #{table_name} where not Status = 'peeled'").length
        num_peeled = db.execute("select * from #{table_name} where Status = 'unqueued'").length
        # num_obsids = obsids_in_table.length
        # if num_peeled == num_obsids
        if num_peeled == 0
            puts "\nJobs done."
            exit 
        else
            # puts "Number of obsids not yet processed: #{num_obsids - num_peeled}"
            puts "Number of obsids not yet processed: #{num_peeled}"
        end

        puts "Sleeping...\n\n"
        sleep 60
    end

rescue SQLite3::Exception => e 
    puts "Exception occurred: #{e}"

ensure
    db.close if db
end
