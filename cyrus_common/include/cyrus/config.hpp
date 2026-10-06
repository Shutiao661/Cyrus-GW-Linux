// ============================================================================
// config.hpp - 配置解析器 (INI 风格)
// ============================================================================
// 解析简单的 INI 风格配置文件, 不依赖第三方库:
//   [section]       ← 节名 (方括号, 独立一行)
//   key = value     ← 键值对 (等号分隔, 支持行尾注释)
//   # comment       ← 注释行 (# 开头)
//   ; comment       ← 注释行 (; 开头)
//
// 配置示例 (config/gateway.conf):
//   [server]
//   listen_address = 0.0.0.0       # 监听地址, 0.0.0.0 = 所有网卡
//   listen_port = 8080             # 监听端口
//
//   [agent]
//   host = 127.0.0.1               # Agent 服务器地址
//   port = 9999                    # Agent 服务器端口
//   pool_size = 4                  # 连接池大小
//
// 使用方式:
//   Config cfg;
//   cfg.load("config/gateway.conf");
//   auto port = cfg.get_int("server", "listen_port", 8080);
//   auto addr = cfg.get("agent", "host", "127.0.0.1");
// ============================================================================

#pragma once

#include "types.hpp"
#include "logger.hpp"

#include <string>
#include <string_view>
#include <unordered_map>
#include <fstream>
#include <sstream>
#include <cctype>

namespace cyrus {

// ============================================================================
// Config 类
// ============================================================================
// 内部数据结构: map[section][key] = value
// 三层嵌套: section (节) → key (键) → value (值)
// ============================================================================
class Config {
public:
    // --- 加载配置文件 ---
    // filepath: 配置文件路径
    // 返回 true 表示成功, false 表示文件无法打开
    bool load(const std::string& filepath) {
        std::ifstream file(filepath);
        if (!file.is_open()) {
            LOG_ERROR("Cannot open config file: {}", filepath);
            return false;
        }

        std::string current_section;  // 当前所在的节 (默认为空, 即全局节)
        int line_number = 0;

        std::string line;
        while (std::getline(file, line)) {
            line_number++;

            // 去除行首和行尾空白
            line = trim(line);

            // 跳过空行和注释行 (# 或 ; 开头)
            if (line.empty() || line[0] == '#' || line[0] == ';') {
                continue;
            }

            // 检查是否是节声明 [section_name]
            if (line[0] == '[' && line.back() == ']') {
                current_section = line.substr(1, line.size() - 2);
                current_section = trim(current_section);
                continue;
            }

            // 解析 key = value
            auto eq_pos = line.find('=');
            if (eq_pos == std::string::npos) {
                LOG_WARN("{}:{} - invalid line (no '='): {}", filepath, line_number, line);
                continue;
            }

            std::string key = trim(line.substr(0, eq_pos));
            std::string value = trim(line.substr(eq_pos + 1));

            // 去除行尾注释 (value 中间的 # 不被视为注释)
            // 简单策略: 查找第一个空格后的 #, 且 # 前有空格
            auto hash_pos = value.find(" #");
            if (hash_pos != std::string::npos) {
                value = value.substr(0, hash_pos);
                value = trim(value);
            }

            if (!key.empty()) {
                data_[current_section][key] = value;
            }
        }

        LOG_INFO("Loaded config from {}: {} sections", filepath, data_.size());
        return true;
    }

    // --- 读取值 ---

    // 获取字符串值 (带默认值)
    std::string get(const std::string& section,
                    const std::string& key,
                    const std::string& default_val = "") const
    {
        auto sec_it = data_.find(section);
        if (sec_it == data_.end()) return default_val;
        auto key_it = sec_it->second.find(key);
        if (key_it == sec_it->second.end()) return default_val;
        return key_it->second;
    }

    // 获取整数值 (带默认值)
    int get_int(const std::string& section,
                const std::string& key,
                int default_val = 0) const
    {
        auto s = get(section, key, "");
        if (s.empty()) return default_val;
        try {
            return std::stoi(s);
        } catch (...) {
            LOG_WARN("Config [{}/{}]: invalid integer '{}', using default {}",
                     section, key, s, default_val);
            return default_val;
        }
    }

private:
    // --- 去除字符串首尾空白 ---
    static std::string trim(const std::string& s) {
        auto start = std::find_if_not(s.begin(), s.end(),
            [](unsigned char c) { return std::isspace(c); });
        auto end = std::find_if_not(s.rbegin(), s.rend(),
            [](unsigned char c) { return std::isspace(c); }).base();
        return (start < end) ? std::string(start, end) : std::string();
    }

    // 配置数据结构: section → key → value
    std::unordered_map<std::string,
        std::unordered_map<std::string, std::string>> data_;
};

} // namespace cyrus
