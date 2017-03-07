#!/usr/bin/env ruby

require "sqlite3"
require "optparse"

options = {}
OptionParser.new do |opts|
    opts.banner = "Usage: #{__FILE__} [-d /path/to/database]\nDisplay the contents of a database."
    opts.on("-h", "--help", "Display this message.") {puts opts; exit}

    $database = "#{ENV["MYGROUP"]}/obsids.sqlite"
	opts.on("-d", "--database DATABASE", "Specify the database to be used. By default, this is #{$database}") {|o| $database = o}

	opts.on("-v", "--verbose", "List all columns of the database in the output.") {|o| options[:verbose] = o}
end.parse!

# Multiple tables can be used in the same database, but for now assume it's always the same one.
table_name = "downloads"

begin
    abort("Database (#{$database}) doesn't exist!") unless File.exists?($database)
    db = SQLite3::Database.open $database
    db.results_as_hash = true

    db.execute("select * from #{table_name}") do |r|
        if options[:verbose]
            puts r
        else
            puts "#{r["Obsid"]} \t #{r["Status"]} \t #{r["LastChecked"]} \t #{r["Stdout"]}"
        end
    end

rescue SQLite3::Exception => e 
    puts "Exception occurred: #{e}"

ensure
    db.close if db
end
