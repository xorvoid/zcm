#pragma once

#include <string>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

class SharedPage
{
  private:
    std::string name;
    void* shmbuf = nullptr;

    SharedPage(const std::string& name, void* shmbuf) : name(name), shmbuf(shmbuf) {}

  public:
    static SharedPage* newPage(const std::string& name, size_t size)
    {
        int fd = shm_open(name.c_str(),
                          O_CREAT | O_EXCL | O_RDWR,
                          S_IRUSR | S_IWUSR);
        if (fd == -1) {
            if (errno != EEXIST) return nullptr;

            fd = shm_open(name.c_str(), O_RDWR, S_IRUSR | S_IWUSR);
            if (fd == -1) return nullptr;
        }

        if (ftruncate(fd, size) == -1)
            return nullptr;

        void *shmbuf = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

        if (shmbuf == MAP_FAILED)
            return nullptr;

        return new SharedPage(name, shmbuf);
    }

    ~SharedPage() { unlink(name.c_str()); }

    template <typename T>
    T* getBuf() const { return (T*) shmbuf; }
};
