// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "http/web_page_handler.h"

#include <boost/bind.hpp>
#include <boost/mem_fn.hpp>

#include "common/config.h"
#include "env/env.h"
#include "gutil/stl_util.h"
#include "gutil/strings/substitute.h"
#include "http/ev_http_server.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "http/http_status.h"
#include "http/utils.h"
#include "olap/file_helper.h"
#include "util/cpu_info.h"
#include "util/debug_util.h"
#include "util/disk_info.h"
#include "util/mem_info.h"
#include "util/mustache/mustache.h"

using strings::Substitute;

namespace doris {

static std::string s_html_content_type = "text/html";

WebPageHandler::WebPageHandler(EvHttpServer* server) : _http_server(server) {
    _www_path = std::string(getenv("DORIS_HOME")) + "/www/";//getenv()函数返回环境变量路径

    // Make WebPageHandler to be static file handler, static files, e.g. css, png, will be handled by WebPageHandler.
    _http_server->register_static_file_handler(this);

    TemplatePageHandlerCallback root_callback = boost::bind<void>(boost::mem_fn(&WebPageHandler::root_handler), this, _1, _2);
    register_template_page("/", "Home", root_callback, false /* is_on_nav_bar */);//注册模板页面的请求，当请求为host:port时的回调函数为root_handler()
}

WebPageHandler::~WebPageHandler() {
    STLDeleteValues(&_page_map);
}

/*注册模板页面的请求*/
void WebPageHandler::register_template_page(const std::string& path, const string& alias,
                                            const TemplatePageHandlerCallback& callback, bool is_on_nav_bar) {
    // Relative path which will be used to find .mustache file in _www_path
    string render_path = (path == "/") ? "/home" : path;
    auto wrapped_cb = [=](const ArgumentMap& args, std::stringstream* output) {
        EasyJson ej;
        callback(args, &ej);//调用回调函数为ej赋值，ej中包含了页面中需要的各项信息的内容
        render(render_path, ej, true /* is_styled */, output);//使用ej和特定目录下的.mustache模板渲染页面
    };
    register_page(path, alias, wrapped_cb, is_on_nav_bar);//注册页面请求
}

/*注册页面请求*/
void WebPageHandler::register_page(const std::string& path, const string& alias,
                                   const PageHandlerCallback& callback, bool is_on_nav_bar) {
    boost::mutex::scoped_lock lock(_map_lock);
    CHECK(_page_map.find(path) == _page_map.end());
    // first time, register this to web server
    _http_server->register_handler(HttpMethod::GET, path, this);//注册页面请求，当web发送host:port/{path}请求时，进行响应
    _page_map[path] = new PathHandler(true /* is_styled */, is_on_nav_bar, alias, callback);
}

void WebPageHandler::handle(HttpRequest* req) {
    LOG(INFO) << req->debug_string();

    PathHandler* handler = nullptr;
    {
        boost::mutex::scoped_lock lock(_map_lock);
        auto iter = _page_map.find(req->raw_path());
        if (iter != _page_map.end()) {
            handler = iter->second;
        }
    }

    if (handler == nullptr) {
        // Try to handle static file request
        do_file_response(_www_path + req->raw_path(), req);
        // Has replied in do_file_response, so we return here.
        return;
    }

    const auto& params = *req->params();

    // Should we render with css styles?
    bool use_style = (params.find("raw") == params.end());

    std::stringstream content;
    handler->callback()(params, &content);

    std::string output;
    if (use_style) {
        std::stringstream oss;
        render_main_template(content.str(), &oss);//渲染主模板
        output = oss.str();
    } else {
        output = content.str();
    }

    req->add_output_header(HttpHeaders::CONTENT_TYPE, s_html_content_type.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, output);
}

/*定义主模板*/
static const std::string kMainTemplate = R"(
<!DOCTYPE html>
<html>
  <head>
    <title>Doris</title>
    <meta charset='utf-8'/>
    <link href='/bootstrap/css/bootstrap.min.css' rel='stylesheet' media='screen' />
    <link href='/bootstrap/css/bootstrap-table.min.css' rel='stylesheet' media='screen' />
    <script src='/jquery-3.2.1.min.js' defer></script>
    <script src='/bootstrap/js/bootstrap.min.js' defer></script>
    <script src='/bootstrap/js/bootstrap-table.min.js' defer></script>
    <script src='/doris.js' defer></script>
    <link href='/doris.css' rel='stylesheet' />
  </head>
  <body>
    <nav class="navbar navbar-default">
      <div class="container-fluid">
        <div class="navbar-header">
          <a class="navbar-brand" style="padding-top: 5px;" href="/">
            <img src="/logo.png" width='40' height='40' alt="Doris" />
          </a>
        </div>
        <div id="navbar" class="navbar-collapse collapse">
          <ul class="nav navbar-nav">
           {{#path_handlers}}
            <li><a class="nav-link"href="{{path}}">{{alias}}</a></li>
           {{/path_handlers}}
          </ul>
        </div><!--/.nav-collapse -->
      </div><!--/.container-fluid -->
    </nav>
      {{^static_pages_available}}
      <div style="color: red">
        <strong>Static pages not available. Make sure ${DORIS_HOME}/www/ exists and contains web static files.</strong>
      </div>
      {{/static_pages_available}}
      {{{content}}}
    </div>
    {{#footer_html}}
    <footer class="footer"><div class="container text-muted">
      {{{.}}}
    </div></footer>
    {{/footer_html}}
  </body>
</html>
)";

/*获取.mustache文件的tag： {{> 文件名.mustache}}*/
std::string WebPageHandler::mustache_partial_tag(const std::string& path) const {
    return Substitute("{{> $0.mustache}}", path);
}

/*判断静态页面是否存在*/
bool WebPageHandler::static_pages_available() const {
    bool is_dir = false;
    return Env::Default()->is_directory(_www_path, &is_dir).ok() && is_dir;
}

/*判断静态页面以及/home/work/app/doris/tjwqstaging-hdd-tablets/be/package/be/www/目录下对应的.mustache文件是否存在*/
bool WebPageHandler::mustache_template_available(const std::string& path) const {
    if (!static_pages_available()) {//判断静态页面是否存在
        return false;
    }
    return Env::Default()->path_exists(Substitute("$0/$1.mustache", _www_path, path)).ok();//判断/home/work/app/doris/tjwqstaging-hdd-tablets/be/package/be/www/目录下对应的.mustache文件是否存在
}

/*渲染主模板*/
void WebPageHandler::render_main_template(const std::string& content, std::stringstream* output) {
    static const std::string& footer = std::string("<pre>") + get_version_string(true) + std::string("</pre>");

    EasyJson ej;
    ej["static_pages_available"] = static_pages_available();
    ej["content"] = content;
    ej["footer_html"] = footer;
    EasyJson path_handlers = ej.Set("path_handlers", EasyJson::kArray);
    for (const auto& handler : _page_map) {
        if (handler.second->is_on_nav_bar()) {
            EasyJson path_handler = path_handlers.PushBack(EasyJson::kObject);
            path_handler["path"] = handler.first;
            path_handler["alias"] = handler.second->alias();
        }
    }
    mustache::RenderTemplate(kMainTemplate, _www_path, ej.value(), output);//渲染模板
}

/*页面渲染*/
void WebPageHandler::render(const string& path, const EasyJson& ej, bool use_style, std::stringstream* output) {
    if (mustache_template_available(path)) {//判断静态页面以及/home/work/app/doris/tjwqstaging-hdd-tablets/be/package/be/www/目录下对应的.mustache文件是否存在
        mustache::RenderTemplate(mustache_partial_tag(path), _www_path, ej.value(), output);//渲染模板
    } else if (use_style) {
        (*output) << "<pre>" << ej.ToString() << "</pre>";
    } else {
        (*output) << ej.ToString();
    }
}

/*请求为host:port时的回调函数*/
void WebPageHandler::root_handler(const ArgumentMap& args, EasyJson* output) {
    (*output)["version"] = get_version_string(false);//获取编译的版本信息
    (*output)["cpuinfo"] = CpuInfo::debug_string();
    (*output)["meminfo"] = MemInfo::debug_string();
    (*output)["diskinfo"] = DiskInfo::debug_string();
}

} // namespace doris
