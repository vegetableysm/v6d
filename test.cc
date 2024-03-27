#include <fstream>
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>
#include <string>
#include <thread>
#include <vector>

constexpr int THREAD_NUM = 50;

// void threadFunc() {
//   std::string lockPath = "/tmp/llm_cache.lock";
//   std::cout << "lock:" << lockPath << std::endl;

//   int lockFD = open(lockPath.c_str(), O_CREAT | O_RDWR, 0666);
//   if (lockFD < 0) {
//     std::cout << "Failed to open lock file" << std::endl;
//     return;
//   }

//   struct flock fl;
//   fl.l_type = F_WRLCK;
//   fl.l_whence = SEEK_SET;
//   fl.l_start = 0;
//   fl.l_len = 0;
//   do {
//     if (fcntl(lockFD, F_SETLK, &fl) == -1) {
//       std::cout << "Failed to lock file" << std::endl;
//       continue;
//     }
//     std::cout << "lock file success" << std::endl;
//     sleep(5);
//     break;
//   } while(1);

//   std::cout << "unlock" << std::endl;
//   fl.l_type = F_UNLCK;
//   fl.l_whence = SEEK_SET;
//   fl.l_start = 0;
//   fl.l_len = 0;
//   fcntl(lockFD, F_SETLK, &fl);
// }

// int main() {

//   std::vector<std::thread> threads;
//   for (int i = 0; i < 2; i++) {
//     threads.push_back(std::thread(threadFunc));
//   }

//   for (int i = 0; i < 2; i++) {
//     threads[i].join();
//   }

//   return 0;
// }

#include <filesystem>

void threadWrite(int i) {
  char* a = (char *)malloc(30*1024*1024);
  std::string path = "/home/yuansm/refactor/test/" + std::to_string(i) + "/test.txt";
  std::filesystem::create_directories("/home/yuansm/refactor/test/" + std::to_string(i));
  std::fstream file(path, std::ios_base::out | std::ios_base::binary);
  if (!file.is_open()) {
    std::cout << "Failed to open file" << std::endl;
    return;
  }

  file.write(a, 1024*1024*1024);
  file.close();
  int fd = open(path.c_str(), O_RDWR);
  fsync(fd);
  close(fd);
}

void threadRead(int i) {
  char* a = (char *)malloc(30*1024*1024);
  std::string path = "/home/yuansm/refactor/test/" + std::to_string(i) + "/test.txt";
  std::fstream file(path, std::ios_base::in | std::ios_base::binary);
  file.open(path, std::ios_base::in | std::ios_base::binary);
  file.read(a, 20*1024*1024);
  file.close();
}

int main() {
  // std::string path = "/home/yuansm/refactor/test.txt";
  // std::string destination_path = "/home/yuansm/refactor/test/test.txt";
  // // while(1) {
  //   // std::filesystem::move(path, destination_path, std::filesystem::copy_options::skip_existing);
  // //   std::filesystem::rename(path, destination_path);
  // // }
  // int ret = link(path.c_str(), destination_path.c_str());
  // std::cout << "ret:" << ret << std::endl;
  // ret = unlink(path.c_str());
  // std::cout << "ret:" << ret << std::endl;
  char* a = (char *)malloc(1500*1024*1024);
  // std::chrono::high_resolution_clock::time_point start, end;

  // std::vector<std::thread> threads;
  // start = std::chrono::high_resolution_clock::now();
  // for (int i = 0; i < THREAD_NUM; i++) {
  //   threads.push_back(std::thread(threadWrite, i));
  // }

  // for (int i = 0; i < THREAD_NUM; i++) {
  //   threads[i].join();
  // }

  // end = std::chrono::high_resolution_clock::now();
  // std::chrono::duration<double> writeTime = end - start;
  // std::cout << "time:" << writeTime.count() << std::endl;
  // std::cout << "write speed:" << (double)1500 / 1024 / writeTime.count() << "GB/s" << std::endl;

  // threads.clear();
  // start = std::chrono::high_resolution_clock::now();
  // for (int i = 0; i < 20; i++) {
  //   threads.push_back(std::thread(threadRead, i));
  // }
  
  // for (int i = 0; i < 20; i++) {
  //   threads[i].join();
  // }

  // end = std::chrono::high_resolution_clock::now();
  // std::chrono::duration<double> readTime = end - start;
  // std::cout << "time:" << readTime.count() << std::endl;
  // std::cout << "read speed:" << (double)1 / readTime.count() << "GB/s" << std::endl;

  // std::chrono::high_resolution_clock::time_point start, end;
  // std::string path = "/home/yuansm/refactor/testfile";
  // int fd = open(path.c_str(), O_CREAT | O_RDWR, 0666);
  // if (fd < 0) {
  //   std::cout << "Failed to open file" << std::endl;
  //   return -1;
  // }
  // uint64_t size = 0;
  // start = std::chrono::high_resolution_clock::now();
  // for (int i = 0; i < 1024; i++) {
  //   for (int j = 0; j < 1024; j++) {
  //     size += read(fd, a, 1024);
  //   }
  // }
  // end = std::chrono::high_resolution_clock::now();
  // std::chrono::duration<double> time = end - start;
  // std::cout << "time:" << time.count() << std::endl;
  // std::cout << "speed:" << (double)size / 1024 / 1024 / 1024 / time.count() << "GB/s" << std::endl;

  // std::chrono::high_resolution_clock::time_point start, end;
  // std::string path = "/home/yuansm/refactor/testfile";
  // start = std::chrono::high_resolution_clock::now();
  // FILE* file = fopen(path.c_str(), "wb");
  // if (file == nullptr) {
  //   std::cout << "Failed to open file" << std::endl;
  //   return -1;
  // }
  // uint64_t writeSize = 0;
  // writeSize += fwrite(a, 1, 1024 * 1024 * 1024, file);
  // fsync(fileno(file));
  // fclose(file);
  // end = std::chrono::high_resolution_clock::now();
  // std::chrono::duration<double> writeTime = end - start;
  // std::cout << "time:" << writeTime.count() << std::endl;
  // std::cout << "speed:" << (double)writeSize / 1024 / 1024 / 1024 / writeTime.count() << "GB/s" << std::endl;

  // file = nullptr;
  // start = std::chrono::high_resolution_clock::now();
  // file = fopen(path.c_str(), "rb");
  // if (file == nullptr) {
  //   std::cout << "Failed to open file" << std::endl;
  //   return -1;
  // }
  // uint64_t readSize = 0;
  // readSize += fread(a, 1, 1024 * 1024 * 1024, file);
  // fclose(file);
  // end = std::chrono::high_resolution_clock::now();
  // std::chrono::duration<double> readTime = end - start;
  // std::cout << "time:" << readTime.count() << std::endl;
  // std::cout << "speed:" << (double)readSize / 1024 / 1024 / 1024 / readTime.count() << "GB/s" << std::endl;

  std::chrono::high_resolution_clock::time_point start, end;
  std::string path = "/home/yuansm/refactor/testfile";
  std::fstream file(path, std::ios_base::out | std::ios_base::binary);
  if (!file.is_open()) {
    std::cout << "Failed to open file" << std::endl;
    return -1;
  }

  start = std::chrono::high_resolution_clock::now();
  file.write(a, 1500*1024*1024);
  file.close();
  int fd = open(path.c_str(), O_RDWR);
  fsync(fd);
  close(fd);
  end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> writeTime = end - start;
  std::cout << "time:" << writeTime.count() << std::endl;
  std::cout << "write speed:" << (double)1500 / 1024 / writeTime.count() << "GB/s" << std::endl;

  file.open(path, std::ios_base::in | std::ios_base::binary);
  start = std::chrono::high_resolution_clock::now();
  file.read(a, 1500*1024*1024);
  end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> readTime = end - start;
  std::cout << "time:" << readTime.count() << std::endl;
  std::cout << "read speed:" << (double)1500 / 1024 / readTime.count() << "GB/s" << std::endl;
  file.close();

  return 0;
}