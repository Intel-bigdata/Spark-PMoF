#!/usr/bin/env bash

function stop_process() {
  PID_FILE_PATH=$1
  if [ check_recorded_pid $PID_FILE_PATH ]; then
      pid=$(cat $PID_FILE_PATH)
      kill -9 $pid
  fi
}

function check_recorded_pid() {
  PID_FILE_PATH=$1
  if [[ -f "${PID_FILE_PATH}" ]]; then
    pid=$(cat $PID_FILE_PATH)
    if ps -p "${pid}" > /dev/null 2>&1; then
      # running according to the record
      return 1
    fi
#    rm -f "${pidfile}" >/dev/null 2>&1
  fi
  # not running
  return 0;
}