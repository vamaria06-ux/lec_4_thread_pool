#include <iostream>
#include <queue>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <vector>
#include <thread>
#include <functional>
#include <chrono>
#include <atomic>
#include <algorithm>

struct th_pool_t
{
  using f_t = std::function<void(void)>;

  explicit th_pool_t(unsigned int k);
  ~th_pool_t();

  void submit(f_t f);
  void shutdown_now();
  void pause();
  void resume();

private:
  void execute(unsigned int id);

  std::queue<f_t> tasks_;
  std::shared_mutex mx_;
  std::condition_variable_any cd_;

  std::vector<std::queue<f_t>> local_;
  std::vector<std::mutex> local_mx_;

  bool stop_{false};
  bool paused_{false};
  bool cancel_{false};
  std::atomic<int> pending_{0};

  std::vector<std::thread> ths_;
};

th_pool_t::th_pool_t(unsigned int k)
  : local_(k), local_mx_(k)
{
  for (unsigned int i = 0; i < k; ++i)
    ths_.emplace_back([this, i]() { execute(i); });
}

void th_pool_t::submit(f_t f)
{
  {
    std::unique_lock lock(mx_);
    if (stop_) return;
    tasks_.push(std::move(f));
    ++pending_;
  }
  cd_.notify_all();
}

th_pool_t::~th_pool_t()
{
  {
    std::unique_lock lock(mx_);
    stop_ = true;
    paused_ = false;
  }
  cd_.notify_all();
  for (auto& th : ths_)
    if (th.joinable()) th.join();
}

void th_pool_t::shutdown_now()
{
  {
    std::unique_lock lock(mx_);
    stop_ = true;
    cancel_ = true;
    paused_ = false;
    std::queue<f_t> empty;
    std::swap(tasks_, empty);
  }
  cd_.notify_all();
  for (auto& th : ths_)
    if (th.joinable()) th.join();
}

void th_pool_t::pause()
{
  std::unique_lock lock(mx_);
  paused_ = true;
}

void th_pool_t::resume()
{
  {
    std::unique_lock lock(mx_);
    paused_ = false;
  }
  cd_.notify_all();
}

void th_pool_t::execute(unsigned int id)
{
  while (true)
  {
    f_t task;

    {
      std::shared_lock lock(mx_);
      if (cancel_) return;
      if (paused_ && !stop_)
      {
        lock.unlock();
        std::unique_lock write(mx_);
        cd_.wait(write, [&] { return !paused_ || stop_; });
      }
    }

    {
      std::lock_guard lock(local_mx_[id]);
      if (!local_[id].empty())
      {
        task = std::move(local_[id].front());
        local_[id].pop();
      }
    }

    if (!task)
    {
      std::shared_lock read(mx_);
      if (!tasks_.empty())
      {
        read.unlock();
        std::unique_lock write(mx_);

        if (!tasks_.empty() && !cancel_)
        {
          std::lock_guard local_lock(local_mx_[id]);

          for (int i = 0; i < 4 && !tasks_.empty(); ++i)
          {
            local_[id].push(std::move(tasks_.front()));
            tasks_.pop();
          }
        }
      }
    }

    if (!task)
    {
      for (unsigned int i = 0; i < local_.size(); ++i)
      {
        if (i == id) continue;

        std::lock_guard lock(local_mx_[i]);

        if (!local_[i].empty())
        {
          task = std::move(local_[i].front());
          local_[i].pop();
          break;
        }
      }
    }

    if (!task)
    {
      std::lock_guard lock(local_mx_[id]);
      if (!local_[id].empty())
      {
        task = std::move(local_[id].front());
        local_[id].pop();
      }
    }

    if (task)
    {
      try { task(); } catch (...) {}
      --pending_;
      cd_.notify_all();
      continue;
    }

    std::unique_lock lock(mx_);

    if (cancel_ || (stop_ && pending_ == 0))
      break;

    cd_.wait_for(lock, std::chrono::milliseconds(2));
  }
}

int main()
{
  std::atomic<int> done{0};

  {
    th_pool_t pool(4);

    for (int i = 0; i < 40; ++i)
    {
      pool.submit([i, &done]()
      {
        std::this_thread::sleep_for(
          std::chrono::milliseconds(i % 5 == 0 ? 100 : 10)
        );
        ++done;
      });
    }
  }

  std::cout << "Completed: " << done << "/40\n";
}
