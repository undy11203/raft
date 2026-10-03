cmake_minimum_required(VERSION 3.28)

project(raft
    VERSION 0.1.0
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_SCAN_FOR_MODULES OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
endif()

# ---------------------------------------------------------------------------
# Boost: скачивается и собирается автоматически (только перечисленные библиотеки
# и их зависимости). Архив кэшируется в build/_deps.
# ---------------------------------------------------------------------------
include(FetchContent)

set(RAFT_BOOST_VERSION 1.89.0)
set(BOOST_INCLUDE_LIBRARIES asio beast filesystem json program_options)
set(BOOST_ENABLE_CMAKE ON)

FetchContent_Declare(Boost
    URL https://github.com/boostorg/boost/releases/download/boost-${RAFT_BOOST_VERSION}/boost-${RAFT_BOOST_VERSION}-cmake.tar.xz
    URL_HASH SHA256=67acec02d0d118b5de9eb441f5fb707b3a1cdd884be00ca24b9a73c995511f74
    DOWNLOAD_EXTRACT_TIMESTAMP ON
    EXCLUDE_FROM_ALL
    SYSTEM
)
FetchContent_MakeAvailable(Boost)

# ---------------------------------------------------------------------------
# Узел Raft
# ---------------------------------------------------------------------------
add_executable(raft_node
    src/main.cpp
)

target_include_directories(raft_node PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

target_link_libraries(raft_node PRIVATE
    Boost::asio
    Boost::beast
    Boost::filesystem
    Boost::json
    Boost::program_options
)

if(WIN32)
    # Windows 10+, иначе Asio предупреждает про _WIN32_WINNT
    target_compile_definitions(raft_node PRIVATE _WIN32_WINNT=0x0A00 WIN32_LEAN_AND_MEAN NOMINMAX)
    target_link_libraries(raft_node PRIVATE ws2_32 mswsock)
endif()

if(MSVC)
    target_compile_options(raft_node PRIVATE /W4 /permissive- /bigobj)
else()
    target_compile_options(raft_node PRIVATE -Wall -Wextra -Wpedantic)
    if(MINGW)
        # Beast/Asio дают очень большие объектные файлы
        target_compile_options(raft_node PRIVATE -Wa,-mbig-obj)
    endif()
endif()
