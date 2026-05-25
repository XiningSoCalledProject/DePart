cmake_minimum_required(VERSION 3.5)
project(SEAL_ORAM_NETIO)

# ===== Build outputs (so you always run the right binary) =====
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Put executables in build/bin, libraries in build/lib
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# (Optional) If you use multi-config generators (Xcode), also set config dirs
foreach(OUTPUTCONFIG IN ITEMS Debug Release RelWithDebInfo MinSizeRel)
	string(TOUPPER ${OUTPUTCONFIG} OUTPUTCONFIG_UPPER)
	set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_${OUTPUTCONFIG_UPPER} ${CMAKE_BINARY_DIR}/bin)
	set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_${OUTPUTCONFIG_UPPER} ${CMAKE_BINARY_DIR}/lib)
	set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_${OUTPUTCONFIG_UPPER} ${CMAKE_BINARY_DIR}/lib)
endforeach()

#include(cmake/common.cmake)

find_package(OpenSSL REQUIRED)
# find_package(EMP REQUIRED)
find_library(EMP_LIBRARIES NAMES emp-tool)
find_library(CRYPTOPP_LIBRARY NAMES cryptopp)
find_library(OPENSSL_LIBRARY NAMES openssl)
find_path(CRYPTOPP_INCLUDE_DIR NAMES cryptopp/osrng.h)
find_path(OPENSSL_INCLUDE_DIR NAMES openssl/ec.h)

include_directories(.)
#include_directories(include)
include_directories(include ${CRYPTOPP_INCLUDE_DIR} ${OPENSSL_INCLUDE_DIR})
include_directories(/usr/local/include)
include_directories(/usr/include)
#include_directories(/opt/homebrew/Cellar/openssl@3/3.1.1_1/include)
include_directories(/opt/homebrew/Cellar/boost/1.82.0_1/include)

link_directories(/usr/local/lib)
link_directories(/usr/lib)
#link_directories(cmake_minimum_required(VERSION 3.3))
#link_directories(/opt/homebrew/Cellar/openssl@3/3.1.1_1/lib)
link_directories(/opt/homebrew/Cellar/boost/1.82.0_1/lib)

# link_libraries("/usr/local/lib/libemp-tool.so")

# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -O3")
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# set(DB_SOURCE_FILES)
set(CSV_FILES
		include/CsvWriter.hpp
		include/TpccGenerator.hpp
		src/CsvWriter.cpp
		src/TpccGenerator.cpp)

set(GENERAL_SOURCE_FILES
		include/ServerConnector.h
		include/NetIOConnector.h
		include/Config.h
		include/Util.h
		include/ORAM.h
		include/ORAMServer.h
		include/csv_reader.h
		include/generate_data.h
		include/TpccSchema.h
		include/Repartition.h
		src/NetIOConnector.cpp
		src/Util.cpp
		src/ORAMServer.cpp
		src/generate_data.cpp
		src/csv_reader.cpp
		src/TpccSchema.cpp
		src/Repartition.cpp)

set(IBSOS_SOURCE_FILES
		include/IBSOS.h
		src/IBSOS.cpp
		src/NetIOConnector.cpp)

set(PATHORAM_SOURCE_FILES
		include/PathORAM.h
		include/RecursivePathORAM.h
		include/RingORAM.h
		include/PathORAMdp.h
		src/PathORAM.cpp
		src/RecursivePathORAM.cpp
		src/PathORAMdp.cpp
		src/RingORAM.cpp
)

set(BINARYORAM_SOURCE_FILES
		include/BinaryORAM.h
		include/RecursiveBinaryORAM.h
		src/BinaryORAM.cpp
		src/RecursiveBinaryORAM.cpp)

set(SRORAM_SOURCE_FILES
		include/SRORAM.h
		src/SRORAM.cpp)

#set(TPORAM_SOURCE_FILES
#        include/PartitionORAM.h
#        include/PartitionORAMInstance.h
#        include/TPPartitionORAMInstance.h
#        include/RecursivePartitionORAM.h
#        include/RecursiveTPPartitionORAMInstance.h
#        src/TPPartitionORAMInstance.cpp
#        src/RecursiveTPPartitionORAMInstance.cpp)

#set(TRANS_FILES
#		include/tableconnection.h
#	)
# ====================================================================
# RingORAM Tests - FIXED VERSION
# ====================================================================

# 1) Core ORAM library (shared by all tests)
add_library(oram_core STATIC
		src/RingORAM.cpp
		src/RingORAM_helper.cpp
		src/NetIOConnector.cpp
		src/Util.cpp
		src/csv_reader.cpp
		src/TpccSchema.cpp
		src/Repartition.cpp
		src/MultiRingORAM_Servers.cpp
		src/Queries_Receiving.cpp
)

target_include_directories(oram_core PUBLIC
		${CMAKE_SOURCE_DIR}/include
		${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(oram_core PUBLIC
		cryptopp
		emp-tool
)

# 2) RingORAM Helper Test (main test)
add_executable(RingORAM_helper_test
		"Testing/Unit Tests/RingORAM_main.cpp"
		include/RingORAM_helper.h
)

target_link_libraries(RingORAM_helper_test PRIVATE
		oram_core
)

target_include_directories(RingORAM_helper_test PRIVATE
		${CMAKE_SOURCE_DIR}/include
		${CMAKE_SOURCE_DIR}/src
)

# 3) Repartition Unit Tests
add_executable(Repartition_UnitTests
		"Testing/Unit Tests/Repartition_Test.cpp"
)

target_link_libraries(Repartition_UnitTests PRIVATE oram_core)
target_include_directories(Repartition_UnitTests PRIVATE
		${CMAKE_SOURCE_DIR}/include
		${CMAKE_SOURCE_DIR}/src
)

# 4) Server (main test)
add_executable(Server
		"Testing/Unit Tests/Server.cpp"
)

target_link_libraries(Server PRIVATE
		oram_core
)

target_include_directories(Server PRIVATE
		${CMAKE_SOURCE_DIR}/include
		${CMAKE_SOURCE_DIR}/src
)

# 5). MultiRingORAM Client (test)
add_executable(MultiRingORAM_Servers_main
		"Testing/Unit Tests/MultiRingORAM_Servers_main.cpp"
)
target_link_libraries(MultiRingORAM_Servers_main PRIVATE oram_core)
target_include_directories(MultiRingORAM_Servers_main PRIVATE
		${CMAKE_SOURCE_DIR}/include
		${CMAKE_SOURCE_DIR}/src
)

# 7). Queries Receiving Test (separate executable — has its own main())
add_executable(Queries_Receiving_Test
		"Testing/Unit Tests/Queries_Receiving_Test.cpp"
)
target_link_libraries(Queries_Receiving_Test PRIVATE oram_core)
target_include_directories(Queries_Receiving_Test PRIVATE
		${CMAKE_SOURCE_DIR}/include
		${CMAKE_SOURCE_DIR}/src
)

# 6). MultiRingORAM Server (separate executable)
add_executable(Servers_MultiRingORAM
		"Testing/Unit Tests/Servers_MultiRingORAM.cpp"
)
target_link_libraries(Servers_MultiRingORAM PRIVATE oram_core)
target_include_directories(Servers_MultiRingORAM PRIVATE
		${CMAKE_SOURCE_DIR}/include
		${CMAKE_SOURCE_DIR}/src
)

# 8). Multi-Client + QueriesReceiving integration test
add_executable(MultiClients_QR_Test
		"Testing/Unit Tests/MultiClients_QR_Test.cpp"
)
target_link_libraries(MultiClients_QR_Test PRIVATE oram_core)
target_include_directories(MultiClients_QR_Test PRIVATE
		${CMAKE_SOURCE_DIR}/include
		${CMAKE_SOURCE_DIR}/src
)

# 9). End-to-End Integration Test — Fixed-Partition-Count (Algorithm 3)
add_executable(EndToEnd_Test
		"Testing/Unit Tests/EndToEnd_Test.cpp"
)
target_link_libraries(EndToEnd_Test PRIVATE oram_core)
target_include_directories(EndToEnd_Test PRIVATE
		${CMAKE_SOURCE_DIR}/include
		${CMAKE_SOURCE_DIR}/src
)

# 10). End-to-End Integration Test — Fixed-Threshold (Algorithm 2)
add_executable(EndToEnd_FixedThreshold_Test
		"Testing/Unit Tests/EndToEnd_FixedThreshold_Test.cpp"
)
target_link_libraries(EndToEnd_FixedThreshold_Test PRIVATE oram_core tpcc_support)
target_include_directories(EndToEnd_FixedThreshold_Test PRIVATE
		${CMAKE_SOURCE_DIR}/include
		${CMAKE_SOURCE_DIR}/src
)

# 11). Insert-Only RingORAM Benchmark — measures fill time vs capacity
add_executable(Insert_Benchmark
		"Testing/Insert-OnlyExperiment/Insert-Only_Test.cpp"
)
target_link_libraries(Insert_Benchmark PRIVATE oram_core)
target_include_directories(Insert_Benchmark PRIVATE
		${CMAKE_SOURCE_DIR}/include
		${CMAKE_SOURCE_DIR}/src
)

# ============================================================================
# TPC-C Support
# ============================================================================

add_library(tpcc_support STATIC
		src/TpccGenerator.cpp
)

target_include_directories(tpcc_support PUBLIC
		${CMAKE_SOURCE_DIR}/include
		${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(tpcc_support PUBLIC
		oram_core
)

# ============================================================================
# TPC-C Benchmark
# ============================================================================

add_executable(TPCC_Main
		"TPC-C/TPCC_Main.cpp"
)

target_link_libraries(TPCC_Main PRIVATE
		oram_core
		tpcc_support
)

target_include_directories(TPCC_Main PRIVATE
		${CMAKE_SOURCE_DIR}/include
		${CMAKE_SOURCE_DIR}/src
)

# ============================================================================
# Experiment 1 — Uniform Distribution
# ============================================================================

# Workload 1: 100% Read
add_executable(ORAM_Performance_100R
		"Testing/Experiment1/ORAM_Performance_100R.cpp"
)
target_link_libraries(ORAM_Performance_100R PRIVATE oram_core)
target_include_directories(ORAM_Performance_100R PRIVATE
		${CMAKE_SOURCE_DIR}/include
		${CMAKE_SOURCE_DIR}/src
)

# Workload 2: Read:Write = 1:1
add_executable(ORAM_Performance_50R50W
		"Testing/Experiment1/ORAM_Performance_50R50W.cpp"
)
target_link_libraries(ORAM_Performance_50R50W PRIVATE oram_core)
target_include_directories(ORAM_Performance_50R50W PRIVATE
		${CMAKE_SOURCE_DIR}/include
		${CMAKE_SOURCE_DIR}/src
)

# Workload 3: Read:Write = 2:1
add_executable(ORAM_Performance_67R33W
		"Testing/Experiment1/ORAM_Performance_67R33W.cpp"
)
target_link_libraries(ORAM_Performance_67R33W PRIVATE oram_core)
target_include_directories(ORAM_Performance_67R33W PRIVATE
		${CMAKE_SOURCE_DIR}/include
		${CMAKE_SOURCE_DIR}/src
)

# Workload 4: 100% Write
add_executable(ORAM_Performance_100W
		"Testing/Experiment1/ORAM_Performance_100W.cpp"
)
target_link_libraries(ORAM_Performance_100W PRIVATE oram_core)
target_include_directories(ORAM_Performance_100W PRIVATE
		${CMAKE_SOURCE_DIR}/include
		${CMAKE_SOURCE_DIR}/src
)

# ============================================================================
# Experiment 2 — Skewed Distribution
# ============================================================================

# Workload 1: 100% Read (Skewed)
add_executable(ORAM_Performance_100R_Skewed
		"Testing/Experiment2/Oram_Performance_100R_Skewed.cpp"
)
target_link_libraries(ORAM_Performance_100R_Skewed PRIVATE oram_core)
target_include_directories(ORAM_Performance_100R_Skewed PRIVATE
		${CMAKE_SOURCE_DIR}/include
		${CMAKE_SOURCE_DIR}/src
)

# Workload 2: Read:Write = 1:1 (Skewed)
add_executable(ORAM_Performance_50R50W_Skewed
		"Testing/Experiment2/Oram_Performance_50R50W_Skewed.cpp"
)
target_link_libraries(ORAM_Performance_50R50W_Skewed PRIVATE oram_core)
target_include_directories(ORAM_Performance_50R50W_Skewed PRIVATE
		${CMAKE_SOURCE_DIR}/include
		${CMAKE_SOURCE_DIR}/src
)

# Workload 3: Read:Write = 2:1 (Skewed)
add_executable(ORAM_Performance_67R33W_Skewed
		"Testing/Experiment2/Oram_Performance_67R33W_Skewed.cpp"
)
target_link_libraries(ORAM_Performance_67R33W_Skewed PRIVATE oram_core)
target_include_directories(ORAM_Performance_67R33W_Skewed PRIVATE
		${CMAKE_SOURCE_DIR}/include
		${CMAKE_SOURCE_DIR}/src
)

# Workload 4: 100% Write (Skewed)
add_executable(ORAM_Performance_100W_Skewed
		"Testing/Experiment2/Oram_Performance_100W_Skewed.cpp"
)
target_link_libraries(ORAM_Performance_100W_Skewed PRIVATE oram_core)
target_include_directories(ORAM_Performance_100W_Skewed PRIVATE
		${CMAKE_SOURCE_DIR}/include
		${CMAKE_SOURCE_DIR}/src
)

# ============================================================================
# Experiment 3 — Data Skewed Distribution
# ============================================================================

# Workload 1: 100% Read (Data Skewed)
add_executable(ORAM_Performance_100R_Data_Skewed
		"Testing/Experiment3/Oram_Performance_100R_Data_Skewed.cpp"
)
target_link_libraries(ORAM_Performance_100R_Data_Skewed PRIVATE oram_core)
target_include_directories(ORAM_Performance_100R_Data_Skewed PRIVATE
		${CMAKE_SOURCE_DIR}/include
		${CMAKE_SOURCE_DIR}/src
)

# Workload 2: Read:Write = 1:1 (Data Skewed)
add_executable(ORAM_Performance_50R50W_Data_Skewed
		"Testing/Experiment3/Oram_Performance_50R50W_Data_Skewed.cpp"
)
target_link_libraries(ORAM_Performance_50R50W_Data_Skewed PRIVATE oram_core)
target_include_directories(ORAM_Performance_50R50W_Data_Skewed PRIVATE
		${CMAKE_SOURCE_DIR}/include
		${CMAKE_SOURCE_DIR}/src
)

# Workload 3: Read:Write = 2:1 (Data Skewed)
add_executable(ORAM_Performance_67R33W_Data_Skewed
		"Testing/Experiment3/Oram_Performance_67R33W_Data_Skewed.cpp"
)
target_link_libraries(ORAM_Performance_67R33W_Data_Skewed PRIVATE oram_core)
target_include_directories(ORAM_Performance_67R33W_Data_Skewed PRIVATE
		${CMAKE_SOURCE_DIR}/include
		${CMAKE_SOURCE_DIR}/src
)

# Workload 4: 100% Write (Data Skewed)
add_executable(ORAM_Performance_100W_Data_Skewed
		"Testing/Experiment3/Oram_Performance_100W_Data_Skewed.cpp"
)
target_link_libraries(ORAM_Performance_100W_Data_Skewed PRIVATE oram_core)
target_include_directories(ORAM_Performance_100W_Data_Skewed PRIVATE
		${CMAKE_SOURCE_DIR}/include
		${CMAKE_SOURCE_DIR}/src
)