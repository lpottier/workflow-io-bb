cmake_minimum_required(VERSION 3.2)
message(STATUS "Cmake version ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}.${CMAKE_PATCH_VERSION}")

project(%s)

add_definitions("-g -Wall -Wno-unused-variable -Wno-unused-private-field")

set(CMAKE_CXX_STANDARD 11)

set(SIMGRID_INSTALL_PATH "/usr/local/" CACHE PATH "The path to SimGrid's installation directory")

# include directories for dependencies and WRENCH libraries
include_directories(src/ ${SIMGRID_INSTALL_PATH}/include /usr/local/include /opt/local/include /usr/local/include/wrench)

# source files
set(SOURCE_FILES
       src/BBWMS.h
       src/BBWMS.cpp
       src/BBJobScheduler.h
       src/BBJobScheduler.cpp
       src/BBSimulator.cpp
       src/BBStorage.h
       src/BBStorage.cpp
       src/PFSStorage.h
       src/PFSStorage.cpp
       src/UtilsPrint.h
       src/UtilsPrint.cpp
       )

# test files
set(TEST_FILES
       )

# wrench library and dependencies
find_library(WRENCH_LIBRARY NAMES wrench)
find_library(WRENCH_PEGASUS_WORKFLOW_PARSER_LIBRARY NAMES wrenchpegasusworkflowparser)
find_library(SIMGRID_LIBRARY NAMES simgrid HINTS ${SIMGRID_INSTALL_PATH}/lib/ NO_DEFAULT_PATH)
if (NOT SIMGRID_LIBRARY)
  message(FATAL_ERROR "SimGrid library not found. Perhaps an invalid SIMGRID_INSTALL_PATH specification (${SIMGRID_INSTALL_PATH}?")
endif()
find_library(PUGIXML_LIBRARY NAMES pugixml)
find_library(LEMON_LIBRARY NAMES emon lemon)
find_library(GTEST_LIBRARY NAMES gtest)

# generating the executable
add_executable(workflow-io-bb ${SOURCE_FILES})
target_link_libraries(workflow-io-bb
                       ${WRENCH_LIBRARY}
                       ${WRENCH_PEGASUS_WORKFLOW_PARSER_LIBRARY}
                       ${SIMGRID_LIBRARY}
                       ${PUGIXML_LIBRARY}
                       ${LEMON_LIBRARY}
                      )

install(TARGETS workflow-io-bb DESTINATION bin)

# generating unit tests
add_executable(unit_tests EXCLUDE_FROM_ALL
                   ${SOURCE_FILES}
                   ${TEST_FILES}
               )
target_link_libraries(unit_tests
                       ${GTEST_LIBRARY} wrench -lpthread -lm
                      )