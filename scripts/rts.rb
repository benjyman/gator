#!/usr/bin/env ruby

# This script is intended to run the RTS for every obsid specified.
# It assumes that the data for each obsid has already been downloaded.

$LOAD_PATH.unshift "#{File.dirname(__FILE__)}/.."
require "gator"
require "optparse"

OptionParser.new do |opts|
    opts.banner = "Usage: #{__FILE__} [options] (obsids)\nSubmit jobs to process each obsid with the RTS."
    opts.on("-h", "--help", "Display this message.") {puts opts; exit}

    $peel_number = 1000
	opts.on("-p", "--peel_number NUM", "Peel this many sources with the RTS. Default: #{$peel_number}") {|o| $peel_number = o.to_i}

    $run_time = 40
	opts.on("-t", "--run_time NUM", "Allow the RTS to run for this many minutes. Default: #{$run_time}") {|o| $run_time = o.to_i}
end.parse!


if __FILE__ == $0
    abort "No obsids supplied!" if ARGV.length == 0

    obtain_obsids(ARGV).each do |o|
        setup_jobid, timestamp = rts_setup(o, peel_number: $peel_number)
        puts setup_jobid
        puts patch_jobid = rts_patch(o, setup_jobid, timestamp, mins: $run_time, peel: true)
    end
end
