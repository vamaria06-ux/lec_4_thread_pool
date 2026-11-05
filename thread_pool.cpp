#include <iostream>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <thread>
#include <functional>

struct th_pool_t
{
  using f_t = std::function< void(void) >;
  explicit th_pool_t(unsigned int k);
  ~th_pool_t();
  void submit(f_t f);

  void shutdown_now();
  void pause();
  void resume();
private:
  void execute();
  std::queue<f_t> tasks_;
  std::mutex mx_;
  std::condition_variable cd_;
  bool stop_{false};
  bool paused_{false};
  std::vector< std::thread > ths_;
};

th_pool_t::th_pool_t(unsigned int k)
{
  auto thf = [this]() { execute(); };
  for (; k; --k) ths_.emplace_back(thf);
}

void th_pool_t::submit(f_t f)
{
  mx_.lock();
  tasks_.push(std::move(f));
  mx_.unlock();
  cd_.notify_one();
}

th_pool_t::~th_pool_t()
{
  mx_.lock();
  stop_ = true;
  paused_ = false;
  mx_.unlock();
  cd_.notify_all();
  for (auto &&th: ths_) th.join();
}

void th_pool_t::shutdown_now()
{
  mx_.lock();
  stop_ = true;
  paused_ = false;
  std::queue<f_t> empty_queue;
  std::swap(tasks_, empty_queue);
  mx_.unlock();
  cd.notify_all();
  for (auto &&th: ths_) th.join();
}

void th_pool_t::pause()
{
  mx_.lock();
  paused_ = true;
  mx_.unlock();
}

void th_pool_t::resume()
{
  mx_.lock();
  paused_ = false;
  cd_.notify_all();
  mx_.unlock();
}

void th_pool_t::execute()
{
  std::unique_lock lck(mx_);
  do
  {
    auto proceed = [&]()
    {
      return stop_ || (!tasks_.empty() && !paused_);
    };
    cd_.wait(lck, proceed);
    if (tasks_.empty()) continue;
    auto task = std::move(tasks_.front());
    tasks_.pop();
    lck.unlock();
    try { task(); } catch (...) {}
    lck.lock();
  } while (!stop_ || !tasks_.empty());
}
