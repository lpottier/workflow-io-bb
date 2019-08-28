/**
 * Copyright (c) 2019. <ADD YOUR HEADER INFORMATION>.
 * Generated with the wrench-init tool.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */
#include <wrench.h>
#include <wrench/tools/pegasus/PegasusWorkflowParser.h>

#include "BBJobScheduler.h"
#include "BBWMS.h"

static bool ends_with(const std::string& str, const std::string& suffix) {
    return str.size() >= suffix.size() && 0 == str.compare(str.size()-suffix.size(), suffix.size(), suffix);
}

int main(int argc, char **argv) {

  // Declaration of the top-level WRENCH simulation object
  wrench::Simulation simulation;

  // Initialization of the simulation
  simulation.init(&argc, argv);

  // Parsing of the command-line arguments for this WRENCH simulation
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <xml platform file> <workflow file>" << std::endl;
    exit(1);
  }

  // The first argument is the platform description file, written in XML following the SimGrid-defined DTD
  char *platform_file = argv[1];
  // The second argument is the workflow description file, written in XML using the DAX DTD
  char *workflow_file = argv[2];


  /* Reading and parsing the workflow description file to create a wrench::Workflow object */
  std::cerr << "Loading workflow..." << std::endl;
  wrench::Workflow *workflow;
  if (ends_with(workflow_file, "dax")) {
      workflow = wrench::PegasusWorkflowParser::createWorkflowFromDAX(workflow_file, "1000Gf");
  } else if (ends_with(workflow_file,"json")) {
      workflow = wrench::PegasusWorkflowParser::createWorkflowFromJSON(workflow_file, "1000Gf");
  } else {
      std::cerr << "Workflow file name must end with '.dax' or '.json'" << std::endl;
      exit(1);
  }
  std::cerr << "The workflow has " << workflow->getNumberOfTasks() << " tasks " << std::endl;
  std::cerr.flush();

  // Reading and parsing the platform description file to instantiate a simulated platform
  simulation.instantiatePlatform(platform_file);

  // Get a vector of all the hosts in the simulated platform
  std::vector<std::string> hostname_list = simulation.getHostnameList();

  // Instantiate a storage service
  std::string storage_host = hostname_list[(hostname_list.size() > 2) ? 2 : 1];
  auto storage_service = simulation.add(new wrench::SimpleStorageService(storage_host, 10000000000000.0));

  // Construct a list of hosts (in this example only one host)
  std::string executor_host = hostname_list[(hostname_list.size() > 1) ? 1 : 0];
  std::vector<std::string> execution_hosts = {executor_host};

  // Create a list of storage services that will be used by the WMS
  std::set<std::shared_ptr<wrench::StorageService>> storage_services;
  storage_services.insert(storage_service);

  // Create a list of compute services that will be used by the WMS
  std::set<std::shared_ptr<wrench::ComputeService>> compute_services;

  std::string wms_host = hostname_list[0];
  // Instantiate a batch service and add it to the simulation
  try {
    auto batch_service = new wrench::BatchComputeService(
          wms_host, hostname_list, 0, {},
          {{wrench::BatchComputeServiceMessagePayload::STOP_DAEMON_MESSAGE_PAYLOAD, 2048}});

    compute_services.insert(simulation.add(batch_service));
  } catch (std::invalid_argument &e) {
    std::cerr << "Error: " << e.what() << std::endl;
    std::exit(1);
  }

  // Instantiate a WMS
  auto wms = simulation.add(
          new BBWMS(std::unique_ptr<BBJobScheduler>(
                  new BBJobScheduler(storage_service)),
                        nullptr, compute_services, storage_services, wms_host));
  wms->addWorkflow(workflow);

  // Instantiate a file registry service
  std::string file_registry_service_host = hostname_list[(hostname_list.size() > 2) ? 1 : 0];
  auto file_registry_service =
          new wrench::FileRegistryService(file_registry_service_host);
  simulation.add(file_registry_service);

  // It is necessary to store, or "stage", input files
  auto input_files = workflow->getInputFiles();
  try {
    simulation.stageFiles(input_files, storage_service);
  } catch (std::runtime_error &e) {
    std::cerr << "Exception: " << e.what() << std::endl;
    return 0;
  }

  // Launch the simulation
  try {
    simulation.launch();
  } catch (std::runtime_error &e) {
    std::cerr << "Exception: " << e.what() << std::endl;
    return 0;
  }

  return 0;
}

