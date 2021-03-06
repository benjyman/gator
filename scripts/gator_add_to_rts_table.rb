#!/usr/bin/env ruby

$LOAD_PATH.unshift "#{File.dirname(__FILE__)}/.."
require "gator"
require "sqlite3"
require "optparse"

OptionParser.new do |opts|
    opts.banner = "Usage: #{__FILE__} [-d /path/to/database]\nAdd obsids to a database for downloading."
    opts.on("-h", "--help", "Display this message.") {puts opts; exit}

    $database = "#{ENV["MYGROUP"]}/obsids.sqlite"
	opts.on("-d", "--database DATABASE", "Specify the database to be used. Default: #{$database}") {|o| $database = o}

    $peel_number = 1000
	opts.on("-p", "--peel_number NUM", "Peel this many sources with the RTS. Default: #{$peel_number}") {|o| $peel_number = o.to_i}

    $run_time = 40
	opts.on("-t", "--run_time NUM", "Allow the RTS to run for this many minutes. Default: #{$run_time}") {|o| $run_time = o.to_i}

    $timestamp = true
	opts.on("--timestamp", "Disable the usage of timestamp directories. Default: #{$timestamp}") {$timestamp = false}

    $patch = true
	opts.on("--patch", "Disable the patch step. Default: #{$patch}") {$patch = false}

    $peel = true
	opts.on("--peel", "Disable the peel step. Default: #{$peel}") {$peel = false}

    $epoch_id = "EPOCH_ID"
        opts.on("--epoch_id EPOCH_ID", "Specify the epoch_ID of these observations. Default: #{$epoch_id}") {|o| $epoch_id = o}
end.parse!

abort("$MYGROUP not defined.") unless ENV["MYGROUP"]
abort("$USER not defined.") unless ENV["USER"]

# Multiple tables can be used in the same database, but for now assume it's always the same one.
$table_name = "downloads"


def insert_row(database, obsid, path, sister_obsid, epoch_id)
    database.execute "INSERT INTO #{$table_name}
                      VALUES(null,
                             #{obsid},
                             '#{path}',
                             'unqueued',
                             '#{Time.now}',
                             0,
                             0,
                             ' ',
                             #{$timestamp ? 1 : 0},
                             #{$patch ? 1 : 0},
                             #{$peel ? 1 : 0},
                             #{$peel_number},
                             ' ',
                             ' ',
                             ' ',
                             #{sister_obsid},
                             '#{epoch_id}')"
end

if File.exists?($database)
    db = SQLite3::Database.open $database
else
    db = SQLite3::Database.new $database
    db.execute "CREATE TABLE #{$table_name}(id INTEGER PRIMARY KEY, Obsid INTEGER, Path TEXT, Status TEXT, LastChecked TEXT, SetupJobID INTEGER, RTSJobID INTEGER, Stdout TEXT, Timestamp INTEGER, Patch INTEGER, Peel INTEGER, PeelNumber INTEGER, IonoMagnitude TEXT, IonoPCA TEXT, IonoQA TEXT, SisterObsid INTEGER, EpochID TEXT)"
end
db.results_as_hash = true

# Add specified obsids.
# Some intelligent code here could determine if the obsid has already been downloaded.
# Hash?
obsids_in_table = db.execute("SELECT * FROM #{$table_name}").map {|r| r["Obsid"]}
obtain_obsids(ARGV).each do |o|
    if obsids_in_table.include? o.to_s[0,10].to_i
        puts "Already in database: #{o}"
        next
    end

    obsids_in_table.push o
    obj = Obsid.new(o)
    obj.obs_type
    if obj.type == "LymanA"
        # One for high and low.
        insert_row(db, o, "high", 0, "#{$epoch_id}")
        insert_row(db, o, "low", 0, "#{$epoch_id}")
    elsif obj.type == "moon"
        main_obsid = o.to_s[0,10].to_i
        sister_obsid = o.to_s[10,20].to_i
        insert_row(db, main_obsid, " ",sister_obsid, "#{$epoch_id}")
    else 
        insert_row(db, o, " ", 0, "#{$epoch_id}")
    end
    puts "Added: #{o} with epoch_ID: #{$epoch_id}"
end
