/**
 * Copyright (c) 2019. Loïc Pottier.
 * Generated with the wrench-init tool.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */
#include <wrench.h>

#include <iostream>
#include <iomanip>
#include <fstream>
#include <sstream>
#include <string>
#include <cstddef>          // for std::size_t

#include <getopt.h>
#include <unistd.h>         // for getpid();

#include <simgrid/s4u.hpp>

#include "BBSimulation.h"
//#include "config.h"
// #include "BBDecision.h"

std::map<std::string, std::string> parse_args(int argc, char **argv);

int main(int argc, char **argv) {
std::map<std::string, double> compute_payload_values;
  // Parsing of the command-line arguments for this WRENCH simulation
  auto args = parse_args(argc, argv);

  // Print parsed argument
  std::for_each(args.begin(), args.end(),
    [](std::pair<std::string, std::string> element){
      std::cerr << element.first << " => " << element.second << std::endl;
    }
  );

  // Declaration of the top-level WRENCH simulation object
  BBSimulation simulation(
            args["jobid"],
            args["id"],
            args["pipeline"],
            args["max-pipeline"],
            args["cores"],
            args["fits"],
            args["bb-type"],
            args["platform"], 
            args["dax"], 
            args["stage-file"], 
            args["scheduler-log"],
            args["makespan"],
            args["output"]);

  // Initialization of the simulation
  simulation.init(&argc, argv);

  //MAKE SURE THE DIR EXIST/HAS PERM

  // Reading and parsing the workflow description file to create a wrench::Workflow object
  // and the platform description file to instantiate a simulated platform
  wrench::Workflow *workflow = simulation.parse_inputs();

  std::map<std::pair<std::string, std::string>, std::vector<simgrid::s4u::Link*>> route;
  route = simulation.create_hosts();

  // printHostStorageAssociationTTY(cs_to_pfs);
  // printHostStorageAssociationTTY(cs_to_bb);

  //printHostRouteTTY(route);

  // Create a list of storage services that will be used by the WMS
  std::set<std::shared_ptr<wrench::StorageService>> storage_services = simulation.instantiate_storage_services();

  // Create a list of compute services that will be used by the WMS
  // Instantiate a bare metal service and add it to the simulation
  std::set<std::shared_ptr<wrench::ComputeService>> compute_services = simulation.instantiate_compute_services();


  //All services run on the main PFS node (by rule PFSHost1)
  //wrench::FileRegistryService* file_registry_service = 
  simulation.instantiate_file_registry_service();


  //////////////////////// Stage the chosen files from PFS to BB -> here heuristics or given
  FileMap_t file_placement_heuristic;
  std::shared_ptr<wrench::StorageService> first_bb_node = *(simulation.getBBServices()).begin();

  // Parse the file containing the list of files to stage in. Create a map
  auto files_to_stages = simulation.parseFilesList(args["stage-file"], simulation.getPFSService(), first_bb_node);

  // Create the hashmap containing the files that will be staged in
  bool stage_fits = (args["fits"] == "1");
  // Recall that if stage_fits is true then all the files produced by resample will be written in BB

  /***********************************
  *                                  *
  ********** SWARP SPECIFIC **********
  *                                  *
  ************************************/

  double temp_tot = 0.0;
  /* Stage files */
  /* ignore the W0- in the beggining , it is mandatory to avoid simgrid complaining about different output files with the same name */
  for (auto f : workflow->getFiles()) {
    // Stage resamp.fits file if asked with --fits
    if (stage_fits && f->getID().find(".w.resamp.fits") != std::string::npos) {
      std::cerr << "[INFO]: Resamp: " << std::left << std::setw(50) << f->getID() << " will be read/written in " << std::left << std::setw(10) << first_bb_node->getHostname() << std::endl;
      file_placement_heuristic.insert(std::make_tuple(f, simulation.getPFSService(), first_bb_node));
      continue;
    }
    // Write in BB output files even if there are not in the list (to have the same setup as real executions)
    if (f->getID().find("coadd") != std::string::npos) {
      std::cerr << "[INFO]: coadd: " << std::left << std::setw(50) << f->getID() << " will be written in " << std::left << std::setw(10) << first_bb_node->getHostname() << std::endl;
      file_placement_heuristic.insert(std::make_tuple(f, simulation.getPFSService(), first_bb_node));
      continue;
    }

    // If not found, files stay in PFS by default
    if (files_to_stages.count(f->getID()) == 0) {
      std::cerr << "[INFO]: " << std::left << std::setw(50) << f->getID() << " not found in " << std::left << std::setw(10) << args["stage-file"] << " This file stays in the PFS." << std::endl;
      file_placement_heuristic.insert(std::make_tuple(f, simulation.getPFSService(), simulation.getPFSService()));
    }
    else {
      // Otherwise if found in the stage file furnished, stage this file in BB
      if (files_to_stages[f->getID()]->getHostname() != "PFSHost1"){
        temp_tot += f->getSize();
      }
      std::cerr << "[INFO]: " << std::left << std::setw(50) << f->getID() << " will be staged in " << std::left << std::setw(10) << files_to_stages[f->getID()]->getHostname() << std::endl;
      file_placement_heuristic.insert(std::make_tuple(f, simulation.getPFSService(), files_to_stages[f->getID()]));
    }
    // std::cout << " " << f->getID() << " " << f->getSize() << " " << amount_of_data_staged << std::endl;
  }

  /***********************************
  *                                  *
  ******** END SWARP SPECIFIC ********
  *                                  *
  ************************************/


  int nb_files_in_bb = 0;
  double amount_of_data_in_bb = 0;

  for (auto alloc : file_placement_heuristic) {
    if (std::get<2>(alloc)->getHostname().find("BB") != std::string::npos) {
      nb_files_in_bb = nb_files_in_bb + 1;
      amount_of_data_in_bb = amount_of_data_in_bb + std::get<0>(alloc)->getSize();
    }
    // std::cout << " " << std::left << std::setw(60) 
    //         << std::get<0>(alloc)->getID()
    //         << std::left << std::setw(20)
    //         << std::get<1>(alloc)->getHostname()
    //         << std::left << std::setw(20)
    //         << std::get<2>(alloc)->getHostname()
    //         << std::left << std::setw(10) 
    //         << std::get<0>(alloc)->getSize()/std::pow(2,20) << std::endl;
}

  // Store some useful information for later
  simulation.setNBFileInBB(nb_files_in_bb);
  simulation.setDataInBB(amount_of_data_in_bb);
  simulation.setDataStaged(temp_tot);

  // std::cout << nb_files_staged << "/" 
  //           << workflow->getFiles().size() 
  //           << " files staged in BB (" 
  //           << amount_of_data_staged/std::pow(2,20) 
  //           << " MB)." << std::endl;

  /* One file in first BB node */ 
  // for (auto f : workflow->getFiles()) {
  //   file_placement_heuristic.insert(std::make_tuple(
  //                                   f, 
  //                                   simulation.getPFSService(), 
  //                                   first_bb_node
  //                                   )
  //                             );
  //   break;
  // }

  /* All BB heuristic : as we can have multiple BB : *(simulation.getBBServices()).begin() */ 
  // for (auto f : workflow->getFiles()) {
  //   file_placement_heuristic.insert(std::make_tuple(
  //                                   f, 
  //                                   simulation.getPFSService(), 
  //                                   *(simulation.getBBServices()).begin()
  //                                   )
  //                             );
  // }

  //printFileAllocationTTY(file_placement_heuristic);
  ////////////////////////

  // EFT schedule_heuristic;
  // InitialFileAlloc initFileAlloc(file_placement_heuristic);

  // It is necessary to store, or "stage", input files in the PFS
  //std::pair<int, double> stagein_fstat = 
  simulation.stage_input_files();
  //std::cerr << "Staged "<< stagein_fstat.first << ". Total size:" << stagein_fstat.second << std::endl;

  // auto ftest = *(workflow->getFiles()).begin();
  // initFileAlloc(ftest);

  std::shared_ptr<wrench::WMS> wms = simulation.instantiate_wms_service(file_placement_heuristic);

  // !!! TODO !!! in the doc about Summit ->  (Note that a compute service can be associated to a "by default" storage service upon instantiation);

  //printWorkflowTTY(workflow_id, workflow);
  //printWorkflowFile(workflow_id, workflow, output_dir + "/workflow-stat.csv");
  simulation.dumpWorkflowStatCSV();

  // Launch the simulation
  try {
    simulation.launch();
  } catch (std::runtime_error &e) {
    std::cerr << "Exception: " << e.what() << std::endl;
    return 0;
  }

  bool header = (args["no-header"] == "0");

  printSimulationSummaryTTY(simulation, header);
  printSimulationSummaryCSV(simulation, args["csv"], header);

  //simulation.dumpAllOutputJSON();

  return 0;
}

std::map<std::string, std::string> parse_args(int argc, char **argv) {

  int c;
  std::map<std::string, std::string> args;
  args["fits"] = "0";
  args["no-header"] = "0";

  args["id"] = "0";
  args["jobid"] = std::to_string(getpid());
  args["cores"] = "1";
  args["csv"] = "simu.csv";
  args["pipeline"] = "1";
  args["max-pipeline"] = "1";
  args["bb-type"] = "0";
  args["makespan"] = "0";
  args["scheduler-log"] = "0";
  args["output-dir"] = "0";
  
  int flag_no_header = 0;

  static struct option long_options[] = {
      {"platform",       required_argument, 0, 'p'},
      {"dax",            required_argument, 0, 'x'},
      {"stage-file",     required_argument, 0, 's'},
      {"id",             optional_argument, 0, 'd'},
      {"jobid",          optional_argument, 0, 'j'},
      {"pipeline",       optional_argument, 0, 'n'},
      {"max-pipeline",   optional_argument, 0, 'z'},
      {"cores",          optional_argument, 0, 'c'},
      {"bb-type",        optional_argument, 0, 'b'},
      {"makespan",       optional_argument, 0, 'm'},
      {"scheduler-log",  optional_argument, 0, 'w'},
      {"output-dir",     optional_argument, 0, 'o'},
      {"csv",            optional_argument, 0, 'a'},
      {"fits",           no_argument,       0, 'f'},
      {"help",           no_argument,       0, 'h'},
      {"verbose",        no_argument,       0, 'v'},
      {"no-header",      no_argument,       &flag_no_header,   1},
      {0,                0,                 0,  0 }
  };

  while (1) {
    int option_index = 0;

    c = getopt_long(argc, argv, "hfb:z:j:c:d:p:n:x:s:w:m:o:", long_options, &option_index);
    if (c == -1)
      break;

    std::string name(long_options[option_index].name);

    switch (c) {
      case 'h':
        std::cout << "usage: " << argv[0] << std::endl;
        std::cout << "       [-x | --dax           ]  XML workflow file " << std::endl;
        std::cout << "       [-p | --platform      ]  XML platform file " << std::endl;
        std::cout << "       [-s | --stage-file    ]  List of file to stage in BB " << std::endl;
        std::cout << std::endl;
        std::cout << "       OPTIONNAL:" << std::endl;
        std::cout << "       [-d | --id            ]  Add an JOB ID to the run (a column JOBID). Useful when running multiple simulations." << std::endl;
        std::cout << "       [-j | --jobid         ]  Add an AVG ID to the run (a column AVG). Useful when running multiple simulations." << std::endl;
        std::cout << "       [-n | --pipeline      ]  Number of parallel pipeline in the workflow" << std::endl;
        std::cout << "       [-z | --max-pipeline  ]  Maximum number of parallel pipeline in the workflow" << std::endl;
        std::cout << "       [-c | --cores         ]  Number of cores per tasks (same for all the tasks)" << std::endl;
        std::cout << "       [-b | --bb-type       ]  Burst buffer allocation (PRIVATE, STRIPED or ONNODE)" << std::endl;
        std::cout << "       [-f | --fits          ]  Stage all files produced by RESAMP  in BB ( *.w.resamp.*)" << std::endl;
        std::cout << "       [-m | --makespan      ]  Measured makespan on a real platform " << std::endl;
        std::cout << "       [-w | --scheduler-log ]  Log of this workflow executed on a real platform " << std::endl;
        std::cout << "       [-o | --output-dir    ]  Directory where to output all files produced by the simulation (must exist) " << std::endl;
        std::cout << "       [-a | --csv           ]  CSV output file " << std::endl;
        std::cout << std::endl;
        std::cout << "       [   | --no-header     ]  Do not print header" << std::endl;
        std::cout << "       [-v | --verbose       ]  Verbose output" << std::endl;
        std::cout << "       [-h | --help          ]  Print this help" << std::endl;
        std::exit(1);

      case 'd':
        args[name] = optarg;
        break;

      case 'a':
        args[name] = optarg;
        break;

      case 'j':
        args[name] = optarg;
        break;

      case 'b':
        args[name] = optarg;
        break;

      case 'c':
        args[name] = optarg;
        break;

      case 'n':
        args[name] = optarg;
        break;

      case 'p':
        args[name] = optarg;
        break;

      case 'x':
        args[name] = optarg;
        break;

      case 's':
        args[name] = optarg;
        break;

      case 'm':
        args[name] = optarg;
        break;

      case 'w':
        args[name] = optarg;
        break;

      case 'o':
        args[name] = optarg;
        break;

      case 'f':
        args[name] = "1";
        break;

      case 'z':
        args[name] = optarg;
        break;

      case '?':
        std::cout << "usage: " << argv[0] << std::endl;
        std::cout << "       [-x | --dax           ]  XML workflow file " << std::endl;
        std::cout << "       [-p | --platform      ]  XML platform file " << std::endl;
        std::cout << "       [-s | --stage-file    ]  List of file to stage in BB " << std::endl;
        std::cout << std::endl;
        std::cout << "       OPTIONNAL:" << std::endl;
        std::cout << "       [-d | --id            ]  Add an JOB ID to the run (a column JOBID). Useful when running multiple simulations." << std::endl;
        std::cout << "       [-j | --jobid         ]  Add an AVG ID to the run (a column AVG). Useful when running multiple simulations." << std::endl;
        std::cout << "       [-n | --pipeline      ]  Number of parallel pipeline in the workflow" << std::endl;
        std::cout << "       [-z | --max-pipeline  ]  Maximum number of parallel pipeline in the workflow" << std::endl;
        std::cout << "       [-c | --cores         ]  Number of cores per tasks (same for all the tasks)" << std::endl;
        std::cout << "       [-b | --bb-type       ]  Burst buffer allocation (PRIVATE, STRIPED or ONNODE)" << std::endl;
        std::cout << "       [-f | --fits          ]  Stage all files produced by RESAMP  in BB ( *.w.resamp.*)" << std::endl;
        std::cout << "       [-m | --makespan      ]  Measured makespan on a real platform " << std::endl;
        std::cout << "       [-w | --scheduler-log ]  Log of this workflow executed on a real platform " << std::endl;
        std::cout << "       [-o | --output-dir    ]  Directory where to output all files produced by the simulation (must exist) " << std::endl;
        std::cout << "       [-a | --csv           ]  CSV output file " << std::endl;
        std::cout << std::endl;
        std::cout << "       [   | --no-header     ]  Do not print header" << std::endl;
        std::cout << "       [-v | --verbose       ]  Verbose output" << std::endl;
        std::cout << "       [-h | --help          ]  Print this help" << std::endl;
        std::exit(1);

      // default:
        //std::cout << "?? getopt returned character code 0 "<< c << "??"<< std::endl;
        //std::exit(1);
    }
  }

  if (optind < argc) {
    std::cerr << "non-option ARGV-elements: " << std::endl;
    while (optind < argc)
      std::cerr << argv[optind++] << " " << std::endl;
  }

  if (flag_no_header == 1) {
    args["no-header"] = "1";
  }

  return args;
}

