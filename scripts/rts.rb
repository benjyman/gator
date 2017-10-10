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

    $patch = true
	opts.on("--patch", "Disable the patch step. Default: #{$patch}") {|o| $patch = false}

    $peel = true
	opts.on("--peel", "Disable the peel step. Default: #{$peel}") {|o| $peel = false}
end.parse!


# Sanity checks.
# We can't peel if we're not patching.
if $peel and not $patch
    abort "Cannot peel if we are not patching; exiting."
end

if __FILE__ == $0
    abort "No obsids supplied!" if ARGV.length == 0

    obtain_obsids(ARGV).each do |obsid|
        o = Obsid.new(obsid)
        o.rts(setup_mins: 5,
              cal_mins: $run_time,
              patch: true,
              peel: true,
              peel_number: $peel_number,
              timestamp: true,
              rts_path: "/group/mwaeor/CODE/RTS/bin/rts_gpu")
        puts "#{obsid}: #{o.type}"
    end
end
