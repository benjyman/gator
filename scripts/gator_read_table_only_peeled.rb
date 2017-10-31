#!/usr/bin/env ruby

require "sqlite3"
require "optparse"

OptionParser.new do |opts|
    opts.banner = "Usage: #{__FILE__} [-d /path/to/database]\nDisplay the contents of a database."
    opts.on("-h", "--help", "Display this message.") {puts opts; exit}

    $database = "#{ENV["MYGROUP"]}/obsids.sqlite"
	opts.on("-d", "--database DATABASE", "Specify the database to be used. By default, this is #{$database}") {|o| $database = o}
end.parse!

# Multiple tables can be used in the same database, but for now assume it's always the same one.
table_name = "downloads"

begin
    abort("Database (#{$database}) doesn't exist!") unless File.exists?($database)
    db = SQLite3::Database.open $database
    db.results_as_hash = true

    db.execute("select * from #{table_name}") do |r|
        if r["Status"] == "peeled"
            puts r["Path"]
        end
    end

rescue SQLite3::Exception => e
    puts "#{e.class}: #{e}"

ensure
    db.close if db
end
