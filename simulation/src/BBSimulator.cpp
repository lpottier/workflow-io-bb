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
#include <iomanip>
#include <limits>

#include <simgrid/s4u.hpp>

#include "BBJobScheduler.h"
#include "BBWMS.h"

#define COMPUTE_NODE "compute"
#define STORAGE_NODE "storage"
#define PFS_NODE "PFS"
#define BB_NODE "BB"

typedef std::numeric_limits< double > dbl;

static bool ends_with(const std::string& str, const std::string& suffix) {
    return str.size() >= suffix.size() && 0 == str.compare(str.size()-suffix.size(), suffix.size(), suffix);
}

int main(int argc, char **argv) {

  std::cout.precision(dbl::max_digits10);

  // Declaration of the top-level WRENCH simulation object
  wrench::Simulation simulation;

  // Initialization of the simulation
  simulation.init(&argc, argv);

  // Parsing of the command-line arguments for this WRENCH simulation
  if (argc != 4) {
    std::cerr << "Usage: " << argv[0] << " <xml platform file> <workflow file> <output dir>" << std::endl;
    exit(1);
  }

  // The first argument is the platform description file, written in XML following the SimGrid-defined DTD
  char *platform_file = argv[1];
  // The second argument is the workflow description file, written in XML using the DAX DTD
  char *workflow_file = argv[2];
  // The third argument is the output directory (where to write the simulation results)
  std::string output_dir(argv[3]);

  //MAKE SURE THE DIR EXIST/HAS PERM

  /* Reading and parsing the workflow description file to create a wrench::Workflow object */
  std::cout << "Loading workflow..." << std::endl;
  wrench::Workflow *workflow;
  if (ends_with(workflow_file, "dax")) {
      workflow = wrench::PegasusWorkflowParser::createWorkflowFromDAX(workflow_file, "1000Gf");
  } else if (ends_with(workflow_file,"json")) {
      workflow = wrench::PegasusWorkflowParser::createWorkflowFromJSON(workflow_file, "1000Gf");
  } else {
      std::cerr << "Workflow file name must end with '.dax' or '.json'" << std::endl;
      exit(1);
  }
  std::cout << "The workflow has " << workflow->getNumberOfTasks() << " tasks " << std::endl;
  auto sizefiles = workflow->getFiles();
  double totsize = 0;
  for (auto f : sizefiles)
    totsize += f->getSize();
  std::cout << "Total files size " << totsize << " Bytes (" << totsize/std::pow(2,40) << " TB)" << std::endl;  
  std::cout.flush();

  // Reading and parsing the platform description file to instantiate a simulated platform
  simulation.instantiatePlatform(platform_file);

  // Get a vector of all the hosts in the simulated platform
  std::vector<std::string> hostname_list = simulation.getHostnameList();
  // std::set<std::string> hostname_set(hostname_list.begin(), hostname_list.end());

  // Create a list of storage services that will be used by the WMS
  std::set<std::shared_ptr<wrench::StorageService>> storage_services;
  std::set<std::shared_ptr<wrench::StorageService>> pfs_storage_services;
  std::set<std::shared_ptr<wrench::StorageService>> bb_storage_services;

  // Construct a list of execution hosts (i.e., compute node)
  std::set<std::string> execution_hosts;

  std::string wms_host;
  std::string file_registry_service_host;

  // TODO: Create a set of BB storage and a set of PFS (or a map)

  //Read all hosts and create a list of compute nodes and storage nodes
  for (auto host : hostname_list) {
    simgrid::s4u::Host* simhost = simgrid::s4u::Host::by_name(host);
    std::string host_type = std::string(simhost->get_property("type")); 
    
    if (host_type == std::string(COMPUTE_NODE)) {
      execution_hosts.insert(host);
    }
    else if (host_type == std::string(STORAGE_NODE)) {
      std::string size = std::string(simhost->get_property("size"));
      std::string category = std::string(simhost->get_property("category"));

      std::cout << category << " " << host_type << " size " << std::stod(size) 
                << " Bytes (" << std::stod(size)/std::pow(2,40) << " TB) " 
                << totsize/std::stod(size) << std::endl;
      std::cout.flush();
      
      auto host_service = simulation.add(new wrench::SimpleStorageService(host, std::stod(size)));
      storage_services.insert(host_service);

      if (category == std::string(PFS_NODE)) {
        pfs_storage_services.insert(host_service);
        wms_host = host;
        file_registry_service_host = host;
      }
      else if (category == std::string(BB_NODE)) {
        bb_storage_services.insert(host_service);
      }
    }
  }

  if (pfs_storage_services.size() != 1)
    throw std::runtime_error("This simulation requires exactly one PFS host");

  if (execution_hosts.empty()) {
    throw std::runtime_error("This simulation requires at least one compute node in the platform file");
  }

  if (storage_services.empty()) {
    throw std::runtime_error("This simulation requires at least two storage nodes in the platform file");
  }
  // Create a list of compute services that will be used by the WMS
  std::set<std::shared_ptr<wrench::ComputeService>> compute_services;

  // Instantiate a bare metal service and add it to the simulation
  try {
    auto baremetal_service = new wrench::BareMetalComputeService(
          wms_host, execution_hosts, 0, {}, {});

    compute_services.insert(simulation.add(baremetal_service));
  } catch (std::invalid_argument &e) {
    std::cerr << "Error: " << e.what() << std::endl;
    std::exit(1);
  }

  //All services run on the main PFS node (by rule PFSHost1)
  // Instantiate a file registry service
  auto file_registry_service = new wrench::FileRegistryService(file_registry_service_host);
  simulation.add(file_registry_service);

  //////////////////////// Stage the chosen files from PFS to BB -> here heuristics
  std::map<wrench::WorkflowFile*, std::shared_ptr<wrench::StorageService> > file_placements;

  for (auto f : workflow->getFiles()) {
    // file_placements[f] = *pfs_storage_services.begin();
    file_placements[f] = *bb_storage_services.begin();
  }

  std::cout << std::left << std::setw(30) 
            << "FILE"
            << std::left << std::setw(20)
            << "STORAGE"
            << std::right << std::setw(20) 
            << "SIZE(MB)" << std::endl;
  std::cout << std::left << std::setw(30) 
            << "----"
            << std::left << std::setw(20)
            << "-------"
            << std::right << std::setw(20) 
            << "--------" << std::endl;

  for (auto alloc : file_placements) {
    std::cout << std::left << std::setw(30) 
              << alloc.first->getID()
              << std::left << std::setw(20)
              << alloc.second->getHostname() 
              << std::right << std::setw(20) 
              << alloc.first->getSize()/std::pow(2,20) << std::endl;
  }
  std::cout.flush();
  ////////////////////////

  // It is necessary to store, or "stage", input files in the PFS
  auto input_files = workflow->getInputFiles();
  try {
    simulation.stageFiles(input_files, *pfs_storage_services.begin());
  } catch (std::runtime_error &e) {
    std::cerr << "Exception: " << e.what() << std::endl;
    return 0;
  }

  // Instantiate a WMS
  auto wms = simulation.add(
          new BBWMS(
            std::unique_ptr<BBJobScheduler>(new BBJobScheduler(file_placements)),
            nullptr, compute_services, 
            storage_services, pfs_storage_services, bb_storage_services,
            file_placements, wms_host)
          );
  wms->addWorkflow(workflow);

  // !!! TODO !!! in the doc about Summit ->  (Note that a compute service can be associated to a "by default" storage service upon instantiation);


  // Launch the simulation
  try {
    simulation.launch();
  } catch (std::runtime_error &e) {
    std::cerr << "Exception: " << e.what() << std::endl;
    return 0;
  }

  auto& simulation_output = simulation.getOutput();
  std::vector<wrench::SimulationTimestamp<wrench::SimulationTimestampTaskCompletion> *> trace_tasks;
  std::vector<wrench::SimulationTimestamp<wrench::SimulationTimestampFileCopyCompletion> *> trace_copyfiles;

  trace_tasks = simulation_output.getTrace<wrench::SimulationTimestampTaskCompletion>();
  trace_copyfiles = simulation_output.getTrace<wrench::SimulationTimestampFileCopyCompletion>();

  // std::cout << "Number of entries in TaskCompletion trace: " << trace_tasks.size() << std::endl;
  double makespan = trace_tasks[0]->getDate();
  for (auto task : trace_tasks)
    makespan = makespan < task->getDate() ? task->getDate() : makespan;

  std::cout << std::left << std::setw(30) 
            << "WORKFLOW"
            << std::left << std::setw(30)
            << "PLATFORM"
            << std::right << std::setw(20) 
            << "MAKESPAN(S)" << std::endl;
  std::cout << std::left << std::setw(30) 
            << "--------"
            << std::left << std::setw(30)
            << "--------"
            << std::right << std::setw(20) 
            << "-----------" << std::endl;
  std::cout << std::left << std::setw(30) 
            << workflow_file
            << std::left << std::setw(30)
            << platform_file
            << std::right << std::setw(20) 
            << makespan << std::endl;

  simulation_output.dumpPlatformGraphJSON(output_dir + "/platform.json");
  simulation_output.dumpWorkflowExecutionJSON(workflow, output_dir + "/execution.json", false);
  // simulation_output->dumpWorkflowExecutionJSON(workflow, output_dir + "/execution-layout.json", true);
  simulation_output.dumpWorkflowGraphJSON(workflow, output_dir + "/workflow.json");

  return 0;
}

