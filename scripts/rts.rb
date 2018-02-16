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


if __FILE__ == $0
    abort "No obsids supplied!" if ARGV.length == 0

    obtain_obsids(ARGV).each do |obsid|
        o = Obsid.new(obsid)
        o.rts(setup_mins: 10,
              cal_mins: $run_time,
              patch: $patch,
              peel: $peel,
              peel_number: $peel_number,
              timestamp: $timestamp,
              srclist: $srclist,
              rts_path: $rts_path)
        puts "#{obsid}: #{o.type}"
    end
end
