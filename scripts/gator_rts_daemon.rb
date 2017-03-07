#!/usr/bin/env ruby

$LOAD_PATH.unshift "#{File.dirname(__FILE__)}/.."
require "gator"
require "sqlite3"

database = "/group/mwaeor/cjordan/rts.sqlite"
table_name = "rts"
max_queue_length = 30


begin
    if File.exists?(database)
        db = SQLite3::Database.open database
    else
        db = SQLite3::Database.new database
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
            elsif not jobs_in_queue.include? r["PeelJobID"] and status == "peeling"
                # For now, always assume the peel was successful.
                status = "peeled"
                status_update = true
            end

            if status_update
                db.execute("UPDATE #{table_name} SET Status = '#{status}' WHERE Obsid = #{obsid}")
                db.execute("UPDATE #{table_name} SET LastChecked = '#{Time.now}' WHERE Obsid = #{obsid}")
            end

            # We can't do anything further if we're already at the maximum queue length.
            break if len_queue >= max_queue_length

            if status == "unqueued"
                setup_jobid = rts_setup(obsid)
                patch_jobid = rts_patch(obsid, setup_jobid)
                peel_jobid = rts_peel(obsid, patch_jobid)
                puts "Submitted #{obsid} as jobs #{setup_jobid}, #{patch_jobid}, #{peel_jobid}."
                db.execute("UPDATE #{table_name} SET SetupJobID = #{setup_jobid} WHERE Obsid = #{obsid}")
                db.execute("UPDATE #{table_name} SET PatchJobID = #{patch_jobid} WHERE Obsid = #{obsid}")
                db.execute("UPDATE #{table_name} SET PeelJobID = #{peel_jobid} WHERE Obsid = #{obsid}")
                db.execute("UPDATE #{table_name} SET LastChecked = '#{Time.now}' WHERE Obsid = #{obsid}")
                len_queue += 3
            end
        end

        # Check the status of all obsids - have they all been downloaded? If so, exit.
        num_downloaded = db.execute("select * from #{table_name} where Status = 'peeled'").length
        num_obsids = obsids_in_table.length
        if num_downloaded == num_obsids
            puts "\nJobs done."
            exit 
        else
            puts "Number of obsids not yet processed: #{num_obsids - num_downloaded}"
        end

        puts "Sleeping...\n\n"
        sleep 60
    end

rescue SQLite3::Exception => e 
    puts "Exception occurred: #{e}"

ensure
    db.close if db
end
