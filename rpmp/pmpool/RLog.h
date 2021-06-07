/*
 * Copyright (c) 2020 Intel
 */

#ifndef PMPOOL_LOG_H_
#define PMPOOL_LOG_H_

#include <memory>

#include "pmpool/Config.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"

class RLog {
 public:
  explicit RLog(string log_path, string log_level) {
    file_log_ = spdlog::basic_logger_mt("file_logger", log_path);
    if (log_level == "debug") {
      file_log_->set_level(spdlog::level::debug);
      file_log_->flush_on(spdlog::level::debug);
    } else if (log_level == "info") {
      file_log_->set_level(spdlog::level::info);
      file_log_->flush_on(spdlog::level::info);
    } else if (log_level == "warn") {
      file_log_->set_level(spdlog::level::warn);
      file_log_->flush_on(spdlog::level::warn);
    } else if (log_level == "error") {
      file_log_->set_level(spdlog::level::err);
      file_log_->flush_on(spdlog::level::err);
    } else {
    }
    console_log_ = spdlog::stdout_color_mt("console");
    console_log_->flush_on(spdlog::level::info);
  }

  std::shared_ptr<spdlog::logger> get_file_log() { return file_log_; }
  std::shared_ptr<spdlog::logger> get_console_log() { return console_log_; }

 private:
  std::shared_ptr<spdlog::logger> file_log_;
  std::shared_ptr<spdlog::logger> console_log_;
};

#endif  //  PMPOOL_LOG_H_
