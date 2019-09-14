/**
 * Copyright (c) 2019. Loïc Pottier.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */
#ifndef MY_BBSTORAGE_H
#define MY_BBSTORAGE_H

#include <wrench-dev.h>

class BBStorage : public wrench::SimpleStorageService {
public:
  BBStorage(const std::string& hostname,
            double capacity,
            double linkspeed,
            const std::shared_ptr<wrench::SimpleStorageService>& root_storage,
            std::map<std::string, std::string> property_list = {},
            std::map<std::string, double> messagepayload_list = {});

  const double getLinkSpeed() const { return this->linkspeed; }
  
  const std::shared_ptr<wrench::SimpleStorageService> getRootStorage() const { 
    return this->root_storage;
  }

  double stageInFiles();
  double stageOutFiles();

private:
    double linkspeed;
    std::shared_ptr<wrench::SimpleStorageService> root_storage;
};

#endif //MY_BBSTORAGE_H

