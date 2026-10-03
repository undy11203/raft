// Проверка, что все подключённые библиотеки Boost компилируются, линкуются и работают.

#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include <boost/json.hpp>
#include <boost/program_options.hpp>
#include <boost/version.hpp>

#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <string>

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace fs = boost::filesystem;
namespace json = boost::json;
namespace po = boost::program_options;

using tcp = asio::ip::tcp;

namespace {

void check(bool ok, const std::string& name) {
    std::cout << (ok ? "[ OK ] " : "[FAIL] ") << name << '\n';
    if (!ok) {
        throw std::runtime_error(name + " check failed");
    }
}

std::uint16_t check_program_options(int argc, char** argv) {
    std::uint16_t port = 0;
    po::options_description desc("Options");
    desc.add_options()
        ("help,h", "show help")
        ("port,p", po::value<std::uint16_t>(&port)->default_value(0), "HTTP port (0 = any free)");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << '\n';
    }
    check(vm.count("port") == 1, "Boost.ProgramOptions");
    return port;
}

void check_filesystem() {
    const fs::path dir = fs::temp_directory_path() / fs::unique_path("raft-check-%%%%-%%%%");
    fs::create_directories(dir);
    const fs::path file = dir / "state.json";
    {
        fs::ofstream out(file);
        out << R"({"term":1})";
    }
    const bool ok = fs::exists(file) && fs::file_size(file) > 0;
    fs::remove_all(dir);
    check(ok && !fs::exists(dir), "Boost.Filesystem");
}

void check_json() {
    json::value v = {{"term", 3}, {"voted_for", 1}, {"entries", json::array{"set x 1", "set y 2"}}};
    const std::string text = json::serialize(v);
    const json::value parsed = json::parse(text);
    check(parsed.at("term").as_int64() == 3 && parsed.at("entries").as_array().size() == 2,
          "Boost.JSON");
}

// Поднимает HTTP-сервер на Beast, отправляет ему запрос клиентом на Beast,
// всё асинхронно на одном io_context + таймер Asio.
void check_asio_beast(std::uint16_t port) {
    asio::io_context ioc;

    // Asio: таймер
    bool timer_fired = false;
    asio::steady_timer timer(ioc, std::chrono::milliseconds(10));
    timer.async_wait([&](beast::error_code ec) { timer_fired = !ec; });

    // Beast: сервер
    tcp::acceptor acceptor(ioc, tcp::endpoint(asio::ip::make_address("127.0.0.1"), port));
    const auto endpoint = acceptor.local_endpoint();

    tcp::socket server_socket(ioc);
    beast::flat_buffer server_buffer;
    http::request<http::string_body> server_req;
    http::response<http::string_body> server_res;

    acceptor.async_accept(server_socket, [&](beast::error_code ec) {
        if (ec) return;
        http::async_read(server_socket, server_buffer, server_req, [&](beast::error_code ec, std::size_t) {
            if (ec) return;
            server_res.result(http::status::ok);
            server_res.version(server_req.version());
            server_res.set(http::field::content_type, "application/json");
            server_res.body() = json::serialize(json::value{{"echo", std::string(server_req.target())}});
            server_res.prepare_payload();
            http::async_write(server_socket, server_res, [&](beast::error_code, std::size_t) {
                server_socket.shutdown(tcp::socket::shutdown_send);
            });
        });
    });

    // Beast: клиент
    beast::tcp_stream client(ioc);
    beast::flat_buffer client_buffer;
    http::request<http::empty_body> client_req(http::verb::get, "/status", 11);
    client_req.set(http::field::host, "127.0.0.1");
    http::response<http::string_body> client_res;

    client.async_connect(endpoint, [&](beast::error_code ec) {
        if (ec) return;
        http::async_write(client, client_req, [&](beast::error_code ec, std::size_t) {
            if (ec) return;
            http::async_read(client, client_buffer, client_res, [&](beast::error_code, std::size_t) {});
        });
    });

    ioc.run_for(std::chrono::seconds(5));

    check(timer_fired, "Boost.Asio (steady_timer)");
    check(client_res.result() == http::status::ok &&
              json::parse(client_res.body()).at("echo").as_string() == "/status",
          "Boost.Asio + Boost.Beast (HTTP on 127.0.0.1:" + std::to_string(endpoint.port()) + ")");
}

}  // namespace

int main(int argc, char** argv) {
    try {
        std::cout << "Boost " << BOOST_VERSION / 100000 << '.' << BOOST_VERSION / 100 % 1000 << '.'
                  << BOOST_VERSION % 100 << '\n';

        const auto port = check_program_options(argc, argv);
        check_filesystem();
        check_json();
        check_asio_beast(port);

        std::cout << "All Boost libraries work.\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << '\n';
        return 1;
    }
}
