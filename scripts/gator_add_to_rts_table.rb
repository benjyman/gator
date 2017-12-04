#!/usr/bin/env ruby

$LOAD_PATH.unshift "#{File.dirname(__FILE__)}/.."
require "gator"
require "sqlite3"
require "optparse"

OptionParser.new do |opts|
    opts.banner = "Usage: #{__FILE__} [-d /path/to/database]\nAdd obsids to a database for downloading."
    opts.on("-h", "--help", "Display this message.") {puts opts; exit}

    $database = "#{ENV["MYGROUP"]}/obsids.sqlite"
	opts.on("-d", "--database DATABASE", "Specify the database to be used. By default, this is #{$database}") {|o| $database = o}

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
end.parse!

abort("$MYGROUP not defined.") unless ENV["MYGROUP"]
abort("$USER not defined.") unless ENV["USER"]

# Multiple tables can be used in the same database, but for now assume it's always the same one.
$table_name = "downloads"


def insert_row(database, obsid, path)
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
                             ' ')"
end

if File.exists?($database)
    db = SQLite3::Database.open $database
else
    db = SQLite3::Database.new $database
    db.execute "CREATE TABLE #{$table_name}(id INTEGER PRIMARY KEY, Obsid INTEGER, Path TEXT, Status TEXT, LastChecked TEXT, SetupJobID INTEGER, RTSJobID INTEGER, Stdout TEXT, Timestamp INTEGER, Patch INTEGER, Peel INTEGER, PeelNumber INTEGER, IonoMagnitude TEXT, IonoPCA TEXT, IonoQA TEXT)"
end
db.results_as_hash = true

# Add specified obsids.
# Some intelligent code here could determine if the obsid has already been downloaded.
# Hash?
obsids_in_table = db.execute("SELECT * FROM #{$table_name}").map {|r| r["Obsid"]}
obtain_obsids(ARGV).each do |o|
    if obsids_in_table.include? o
        puts "Already in database: #{o}"
        next
    end

    obsids_in_table.push o
    obj = Obsid.new(o)
    obj.obs_type
    if obj.type == "LymanA"
        # One for high and low.
        insert_row(db, o, "high")
        insert_row(db, o, "low")
    else
        insert_row(db, o, " ")
    end
    puts "Added: #{o}"
end
