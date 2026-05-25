//
// Auto-Starting Parallel Client
// Automatically spawns multiple server processes and connects to them
// Usage: ./AutoClient [num_servers] [starting_port] [message]
//

#include <iostream>
#include <vector>
#include <thread>
#include <mutex>
#include <chrono>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sstream>
#include <iomanip>
#include <signal.h>

#define BUFFER_SIZE 4096
#define DEFAULT_NUM_SERVERS 3
#define DEFAULT_STARTING_PORT 8881
#define DEFAULT_MESSAGE "Hello from AutoClient"

// Structure to hold server information
struct ServerInfo {
    std::string ip;
    int port;
    int id;
    pid_t pid;  // Process ID of the server
};

// Structure to hold connection result
struct ConnectionResult {
    int server_id;
    std::string server_address;
    bool success;
    std::string response;
    double elapsed_time_ms;
    std::string error_message;
};

// Global variables for cleanup
std::vector<pid_t> server_pids;
std::mutex cout_mutex;

// Signal handler for cleanup
void cleanup_servers(int signum) {
    std::cout << "\n\n🛑 Shutting down servers..." << std::endl;
    for (pid_t pid : server_pids) {
        kill(pid, SIGTERM);
    }
    exit(signum);
}

// Thread-safe console output
void safe_print(const std::string& message) {
    std::lock_guard<std::mutex> lock(cout_mutex);
    std::cout << message << std::flush;
}

// Start a server process
pid_t startServer(int port, const std::string& server_exec) {
    pid_t pid = fork();

    if (pid < 0) {
        std::cerr << "❌ Failed to fork server process for port " << port << std::endl;
        return -1;
    }

    if (pid == 0) {
        // Child process: run the server
        // Redirect output to /dev/null or log file
        std::string log_file = "server_" + std::to_string(port) + ".log";
        freopen(log_file.c_str(), "w", stdout);
        freopen(log_file.c_str(), "w", stderr);

        // Execute server
        std::string port_str = std::to_string(port);
        execl(server_exec.c_str(), server_exec.c_str(), port_str.c_str(), nullptr);

        // If execl fails
        std::cerr << "❌ Failed to execute server: " << server_exec << std::endl;
        exit(1);
    }

    // Parent process: return child PID
    return pid;
}

// Check if server is ready
bool waitForServer(const std::string& ip, int port, int max_attempts = 20) {
    for (int attempt = 0; attempt < max_attempts; ++attempt) {
        int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (sock_fd < 0) {
            continue;
        }

        struct sockaddr_in server_addr;
        std::memset(&server_addr, 0, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(port);
        inet_pton(AF_INET, ip.c_str(), &server_addr.sin_addr);

        // Try to connect
        if (connect(sock_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) == 0) {
            close(sock_fd);
            return true;
        }

        close(sock_fd);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    return false;
}

// Connect to a single server and send/receive data
ConnectionResult connectToServer(const ServerInfo& server, const std::string& message) {
    ConnectionResult result;
    result.server_id = server.id;
    result.server_address = server.ip + ":" + std::to_string(server.port);
    result.success = false;

    auto start_time = std::chrono::high_resolution_clock::now();

    // 1. Create socket
    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        result.error_message = "Failed to create socket";
        return result;
    }

    // Set socket timeout
    struct timeval timeout;
    timeout.tv_sec = 5;
    timeout.tv_usec = 0;
    setsockopt(sock_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(sock_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    // 2. Set up server address
    struct sockaddr_in server_addr;
    std::memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(server.port);

    if (inet_pton(AF_INET, server.ip.c_str(), &server_addr.sin_addr) <= 0) {
        result.error_message = "Invalid server address";
        close(sock_fd);
        return result;
    }

    // 3. Connect to server
    safe_print("  [Server " + std::to_string(server.id) + "] Connecting to "
               + result.server_address + "...\n");

    if (connect(sock_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        result.error_message = "Connection failed";
        close(sock_fd);

        auto end_time = std::chrono::high_resolution_clock::now();
        result.elapsed_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
        return result;
    }

    safe_print("  [Server " + std::to_string(server.id) + "] ✅ Connected!\n");

    // 4. Send message
    ssize_t bytes_sent = send(sock_fd, message.c_str(), message.length(), 0);
    if (bytes_sent < 0) {
        result.error_message = "Failed to send message";
        close(sock_fd);
        return result;
    }

    safe_print("  [Server " + std::to_string(server.id) + "] 📤 Sent "
               + std::to_string(bytes_sent) + " bytes\n");

    // 5. Receive response
    char buffer[BUFFER_SIZE];
    std::memset(buffer, 0, BUFFER_SIZE);

    ssize_t bytes_received = recv(sock_fd, buffer, BUFFER_SIZE - 1, 0);
    if (bytes_received > 0) {
        result.response = std::string(buffer, bytes_received);
        result.success = true;

        safe_print("  [Server " + std::to_string(server.id) + "] 📥 Response: \""
                   + result.response + "\"\n");
    } else if (bytes_received == 0) {
        result.error_message = "Server closed connection";
    } else {
        result.error_message = "Failed to receive response";
    }

    // 6. Close connection
    close(sock_fd);

    auto end_time = std::chrono::high_resolution_clock::now();
    result.elapsed_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();

    return result;
}

// Worker thread function
void workerThread(const ServerInfo& server, const std::string& message,
                  std::vector<ConnectionResult>& results, size_t index) {
    results[index] = connectToServer(server, message);
}

int main(int argc, char* argv[]) {
    std::cout << "\n╔═══════════════════════════════════════════════════════╗\n";
    std::cout << "║      Auto-Starting Parallel Client System            ║\n";
    std::cout << "╚═══════════════════════════════════════════════════════╝\n\n";

    // Parse command line arguments
    int num_servers = DEFAULT_NUM_SERVERS;
    int starting_port = DEFAULT_STARTING_PORT;
    std::string message = DEFAULT_MESSAGE;

    if (argc > 1) num_servers = std::atoi(argv[1]);
    if (argc > 2) starting_port = std::atoi(argv[2]);
    if (argc > 3) message = argv[3];

    if (num_servers < 1 || num_servers > 100) {
        std::cerr << "❌ Invalid number of servers (must be 1-100)\n";
        return 1;
    }

    std::cout << "⚙️  Configuration:\n";
    std::cout << "   Number of Servers: " << num_servers << "\n";
    std::cout << "   Starting Port:     " << starting_port << "\n";
    std::cout << "   Message:           \"" << message << "\"\n\n";

    // Set up signal handler for cleanup
    signal(SIGINT, cleanup_servers);
    signal(SIGTERM, cleanup_servers);

    // Determine server executable path
    std::string server_exec = "./Server";
    if (access(server_exec.c_str(), X_OK) != 0) {
        // Try alternative paths
        if (access("./build/Server", X_OK) == 0) {
            server_exec = "./build/Server";
        } else if (access("../Server", X_OK) == 0) {
            server_exec = "../Server";
        } else {
            std::cerr << "❌ Server executable not found!\n";
            std::cerr << "   Looked in: ./Server, ./build/Server, ../Server\n";
            std::cerr << "   Please compile Server first: make Server\n";
            return 1;
        }
    }

    std::cout << "📦 Using server executable: " << server_exec << "\n\n";

    // Step 1: Start all server processes
    std::cout << "🚀 Step 1: Starting " << num_servers << " server processes...\n";

    std::vector<ServerInfo> servers;
    for (int i = 0; i < num_servers; ++i) {
        ServerInfo server;
        server.ip = "127.0.0.1";
        server.port = starting_port + i;
        server.id = i + 1;

        std::cout << "  Starting Server " << server.id << " on port " << server.port << "... ";

        pid_t pid = startServer(server.port, server_exec);
        if (pid < 0) {
            std::cout << "❌ Failed\n";
            // Cleanup already started servers
            for (pid_t p : server_pids) {
                kill(p, SIGTERM);
            }
            return 1;
        }

        server.pid = pid;
        server_pids.push_back(pid);
        servers.push_back(server);

        std::cout << "✅ (PID: " << pid << ")\n";
    }

    std::cout << "\n⏳ Step 2: Waiting for servers to be ready...\n";

    // Step 2: Wait for all servers to be ready
    bool all_ready = true;
    for (auto& server : servers) {
        std::cout << "  Checking Server " << server.id << " (port " << server.port << ")... ";

        if (waitForServer(server.ip, server.port)) {
            std::cout << "✅ Ready\n";
        } else {
            std::cout << "❌ Not responding\n";
            all_ready = false;
        }
    }

    if (!all_ready) {
        std::cerr << "\n❌ Some servers failed to start properly\n";
        cleanup_servers(1);
        return 1;
    }

    std::cout << "\n✅ All servers are ready!\n\n";

    // Step 3: Connect to all servers in parallel
    std::cout << "🚀 Step 3: Connecting to all servers in parallel...\n\n";

    std::vector<ConnectionResult> results(servers.size());
    std::vector<std::thread> threads;

    auto overall_start = std::chrono::high_resolution_clock::now();

    // Launch worker threads
    for (size_t i = 0; i < servers.size(); ++i) {
        threads.emplace_back(workerThread, std::ref(servers[i]), std::ref(message),
                            std::ref(results), i);
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    auto overall_end = std::chrono::high_resolution_clock::now();
    double overall_time = std::chrono::duration<double, std::milli>(overall_end - overall_start).count();

    // Step 4: Display results
    std::cout << "\n" << std::string(60, '=') << "\n";
    std::cout << "📈 Results Summary\n";
    std::cout << std::string(60, '=') << "\n\n";

    int successful = 0;
    int failed = 0;
    double total_time = 0.0;
    double max_time = 0.0;
    double min_time = 1e9;

    for (const auto& result : results) {
        std::cout << "Server " << result.server_id << " (" << result.server_address << "):\n";

        if (result.success) {
            std::cout << "  ✅ Status: SUCCESS\n";
            std::cout << "  📥 Response: \"" << result.response << "\"\n";
            std::cout << "  ⏱️  Time: " << std::fixed << std::setprecision(2)
                      << result.elapsed_time_ms << " ms\n";
            successful++;
            total_time += result.elapsed_time_ms;
            max_time = std::max(max_time, result.elapsed_time_ms);
            min_time = std::min(min_time, result.elapsed_time_ms);
        } else {
            std::cout << "  ❌ Status: FAILED\n";
            std::cout << "  💬 Error: " << result.error_message << "\n";
            std::cout << "  ⏱️  Time: " << std::fixed << std::setprecision(2)
                      << result.elapsed_time_ms << " ms\n";
            failed++;
        }
        std::cout << "\n";
    }

    // Print statistics
    std::cout << std::string(60, '=') << "\n";
    std::cout << "📊 Statistics\n";
    std::cout << std::string(60, '=') << "\n";
    std::cout << "  Total Servers:      " << servers.size() << "\n";
    std::cout << "  Successful:         " << successful << "\n";
    std::cout << "  Failed:             " << failed << "\n";
    std::cout << "  Success Rate:       " << std::fixed << std::setprecision(1)
              << (servers.size() > 0 ? 100.0 * successful / servers.size() : 0.0) << "%\n";
    std::cout << "\n";

    if (successful > 0) {
        std::cout << "  Timing (successful connections):\n";
        std::cout << "    Min Time:         " << std::fixed << std::setprecision(2)
                  << min_time << " ms\n";
        std::cout << "    Max Time:         " << max_time << " ms\n";
        std::cout << "    Avg Time:         " << (total_time / successful) << " ms\n";
    }

    std::cout << "\n  Overall Execution:  " << std::fixed << std::setprecision(2)
              << overall_time << " ms\n";
    std::cout << std::string(60, '=') << "\n\n";

    // Step 5: Cleanup
    std::cout << "🧹 Step 4: Shutting down servers...\n";
    for (size_t i = 0; i < servers.size(); ++i) {
        std::cout << "  Stopping Server " << servers[i].id << " (PID: " << servers[i].pid << ")... ";
        kill(servers[i].pid, SIGTERM);

        // Wait for process to terminate
        int status;
        waitpid(servers[i].pid, &status, 0);
        std::cout << "✅\n";
    }

    std::cout << "\n✅ All operations completed!\n\n";

    return (failed > 0) ? 1 : 0;
}