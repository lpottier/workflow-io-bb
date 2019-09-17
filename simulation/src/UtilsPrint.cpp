/**
 * Copyright (c) 2019. Loïc Pottier.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include <iostream>
#include <iomanip>
#include <fstream>

#include "UtilsPrint.h"

void printWorkflowTTY(const std::string& workflow_id, 
                      wrench::Workflow* workflow) {

  auto precision = std::cout.precision();
  std::cout.precision(dbl::max_digits10);
  //std::cout.setf( std::ios::fixed, std:: ios::floatfield );

  auto files = workflow->getFiles();
  auto tasks = workflow->getTasks();

  double totsize = 0;
  for (auto f : files)
    totsize += f->getSize();

  double avg_size = totsize/files.size();
  double sd_size = 0.0;

  for (auto f : files)
    sd_size += std::pow(f->getSize() - avg_size, 2);

  sd_size = std::sqrt(sd_size/files.size());

  double avg_flop = workflow->getSumFlops(tasks)/tasks.size();
  double sd_flops = 0.0;

  for (auto t : tasks)
    sd_flops += std::pow(t->getFlops() - avg_flop, 2);

  sd_flops = std::sqrt(sd_flops/tasks.size());

  std::cout << std::left << std::setw(20) << " WORKFLOW " << std::left << std::setw(30) << workflow_id << std::endl;
  std::cout << std::left << std::setw(20) << " TASKS " << std::left << std::setw(30) << workflow->getNumberOfTasks() << std::endl;
  std::cout << std::left << std::setw(20) << " LEVELS " << std::left << std::setw(30) << workflow->getNumLevels() << std::endl;
  std::cout << std::left << std::setw(20) << " OPTOT(FLOP) " << std::left << std::setw(30) << workflow->getSumFlops(tasks) << std::endl;
  std::cout << std::left << std::setw(20) << " OPAVG(FLOP) " << std::left << std::setw(30) << avg_flop << std::endl;
  std::cout << std::left << std::setw(20) << " OPSD(FLOP) " << std::left << std::setw(30) << sd_flops << std::endl;
  std::cout << std::left << std::setw(20) << " OPSD(%) " << std::left << std::setw(30) << sd_flops*100/avg_flop << std::endl;
  std::cout << std::left << std::setw(20) << " FILES " << std::left << std::setw(30) << files.size() << std::endl;
  std::cout << std::left << std::setw(20) << " SIZETOT(B) " << std::left << std::setw(30) << totsize << std::endl;
  std::cout << std::left << std::setw(20) << " SIZEAVG(B) " << std::left << std::setw(30) << avg_size << std::endl;
  std::cout << std::left << std::setw(20) << " SIZESD(B) " << std::left << std::setw(30) << sd_size << std::endl;
  std::cout << std::left << std::setw(20) << " SIZESD(%) " << std::left << std::setw(30) << sd_size*100/avg_size << std::endl;

  std::cout.flush();

  //back to previous precision
  std::cout.precision(precision);

}

void printWorkflowFile(const std::string& workflow_id, 
                       wrench::Workflow* workflow, 
                       const std::string& output,
                       const char sep) {

  std::ofstream ofs;
  ofs.open(output, std::ofstream::out | std::ofstream::trunc);

  auto files = workflow->getFiles();
  auto tasks = workflow->getTasks();

  double totsize = 0;
  for (auto f : files)
    totsize += f->getSize();

  double avg_size = totsize/files.size();
  double sd_size = 0.0;

  for (auto f : files)
    sd_size += std::pow(f->getSize() - avg_size, 2);

  sd_size = std::sqrt(sd_size/files.size());

  double avg_flop = workflow->getSumFlops(tasks)/tasks.size();
  double sd_flops = 0.0;

  for (auto t : tasks)
    sd_flops += std::pow(t->getFlops() - avg_flop, 2);

  sd_flops = std::sqrt(sd_flops/tasks.size());

  ofs << "WORKFLOW" << sep << workflow_id << std::endl;
  ofs << "TASKS" << sep << workflow->getNumberOfTasks() << std::endl;
  ofs << "LEVELS" << sep << workflow->getNumLevels() << std::endl;
  ofs << "OPTOT(FLOP)" << sep << workflow->getSumFlops(tasks) << std::endl;
  ofs << "OPAVG(FLOP)" << sep << avg_flop << std::endl;
  ofs << "OPSD(FLOP)" << sep << sd_flops << std::endl;
  ofs << "OPSD(%)" << sep << sd_flops*100/avg_flop << std::endl;
  ofs << "FILES" << sep << files.size() << std::endl;
  ofs << "SIZETOT(B)" << sep << totsize << std::endl;
  ofs << "SIZEAVG(B)" << sep << avg_size << std::endl;
  ofs << "SIZESD(B)" << sep << sd_size << std::endl;
  ofs << "SIZESD(%)" << sep << sd_size*100/avg_size << std::endl;

  ofs.close();

}

void printFileAllocationTTY(const std::map<wrench::WorkflowFile*, std::shared_ptr<wrench::StorageService> >& file_placements) {

  auto precision = std::cout.precision();
  std::cout.precision(dbl::max_digits10);
  //std::cout.setf( std::ios::fixed, std:: ios::floatfield );

  std::cout << std::left << std::setw(31) 
            << " FILE"
            << std::left << std::setw(20)
            << "STORAGE"
            << std::left << std::setw(20) 
            << "SIZE(GB)" << std::endl;
  std::cout << std::left << std::setw(31) 
            << " ----"
            << std::left << std::setw(20)
            << "-------"
            << std::left << std::setw(30) 
            << "--------" << std::endl;

  for (auto alloc : file_placements) {
    std::cout << " " << std::left << std::setw(30) 
              << alloc.first->getID()
              << std::left << std::setw(20)
              << alloc.second->getHostname() 
              << std::left << std::setw(30) 
              << alloc.first->getSize()/std::pow(2,30) << std::endl;
  }
  std::cout.flush();
  //back to previous precision
  std::cout.precision(precision);
}

void printSimulationSummaryTTY(wrench::SimulationOutput& simulation_output) {

  auto precision = std::cout.precision();
  std::cout.precision(dbl::max_digits10);
  //std::cout.setf( std::ios::fixed, std:: ios::floatfield );

  std::vector<wrench::SimulationTimestamp<wrench::SimulationTimestampTaskCompletion> *> trace_tasks = simulation_output.getTrace<wrench::SimulationTimestampTaskCompletion>();

  double makespan = trace_tasks[0]->getDate();
  for (auto task : trace_tasks)
    makespan = makespan < task->getDate() ? task->getDate() : makespan;

  std::cout << std::endl;
  std::cout << std::left << std::setw(20) 
            << "WORKFLOW"
            << std::left << std::setw(20)
            << "PLATFORM"
            << std::left << std::setw(20) 
            << "BBSIZE(GB)"
            << std::left << std::setw(20) 
            << "BBLINK(GB/S)"
            << std::left << std::setw(20) 
            << "MAKESPAN(S)" << std::endl;
  std::cout << std::left << std::setw(20) 
            << "--------"
            << std::left << std::setw(20)
            << "--------"
            << std::left << std::setw(20) 
            << "----------"
            << std::left << std::setw(20) 
            << "------------"
            << std::left << std::setw(20) 
            << "-----------" << std::endl;
  std::cout << std::left << std::setw(20) 
            << "TODO"
            << std::left << std::setw(20)
            << "TODO"
            << std::left << std::setw(20) 
            << "TODO"
            << std::left << std::setw(20) 
            << "TODO"
            << std::left << std::setw(20) 
            << makespan << std::endl;
    //back to previous precision
    std::cout.precision(precision);
}

void printHostRouteTTY(const std::map<std::pair<std::string, std::string>, std::vector<simgrid::s4u::Link*> >& hostpair_to_link) {

  for (auto hostpair: hostpair_to_link) {
    std::cout << "Links from " << hostpair.first.first << " to " << hostpair.first.second << std::endl;
    for (auto link : hostpair.second) {
        std::cout << " | " << link->get_name() 
                  << " at speed " << link->get_bandwidth() 
                  << " latency: " << link->get_latency() << std::endl;
    }
  }

}

void printHostStorageAssociationTTY(const std::map<std::pair<std::string, std::string>, simgrid::s4u::Link*>& map) {

  std::cout << std::left << std::setw(21) 
            << " COMPUTE HOST"
            << std::left << std::setw(20)
            << "STORAGE"
            << std::left << std::setw(20) 
            << "LINK(GB/s)"
            << std::left << std::setw(20) 
            << "LATENCY(uS)" << std::endl;
  std::cout << std::left << std::setw(21) 
            << " ------------"
            << std::left << std::setw(20)
            << "-------"
            << std::left << std::setw(20) 
            << "----------"
            << std::left << std::setw(20) 
            << "-----------" << std::endl;

  for (auto pair : map) {
    std::cout << " " << std::left << std::setw(20) 
              << pair.first.first
              << std::left << std::setw(20)
              << pair.first.second
              << std::left << std::setw(20) 
              << pair.second->get_bandwidth()
              << std::left << std::setw(20) 
              << pair.second->get_latency() << std::endl;
  }
}

