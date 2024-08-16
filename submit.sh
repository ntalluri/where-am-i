directory="logs"
if [ ! -d "$directory" ]; then
  mkdir -p "$directory"
fi

condor_submit submit_files/submit-sleep.sub
condor_submit submit_files/submit-collector.sub