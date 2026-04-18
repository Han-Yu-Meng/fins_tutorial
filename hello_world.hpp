/*******************************************************************************
 * FINS 基础教程：HelloWorld 生产者与消费者
 ******************************************************************************/

#include <atomic>
#include <chrono>
#include <thread>
#include <fins/node.hpp>

/**
 * HelloWorldSource: 数据源节点
 * 负责产生数据流。在 FINS 中，Source 节点通常在内部维护一个线程或响应中断。
 */
class HelloWorldSource : public fins::Node {
public:
  HelloWorldSource() = default;

  /**
   * define(): 节点静态定义
   * 在插件加载时调用，用于描述节点的身份和端口信息。
   * 注意：此阶段不涉及任何资源分配或逻辑执行。
   */
  void define() override {
    set_name("HelloWorldSource");           // 节点名称
    set_description("产生 Hello World 消息"); // 节点描述
    set_category("Tutorials");               // 节点分类

    // register_output<T>(port_name): 注册输出端口
    // 泛型 T 决定了此端口能连接的 Pipe 类型（必须类型匹配）
    register_output<std::string>("str");
  }

  /**
   * initialize(): 初始化
   * 规范：用于分配内存、初始化变量，必须是非阻塞的，不能有耗时操作。
   */
  void initialize() override {
    logger->info("Source 初始化完成.");
    running_ = false;
  }

  /**
   * run(): 启动逻辑
   * 当用户点击“运行”按钮时调用。
   * 规范：必须是非阻塞的！如果需要循环产生数据，必须另起线程（如 worker_）。
   * 不能在这里直接写 while(true)，否则整个调度器会被阻塞。
   */
  void run() override {
    if (running_) return;
    running_ = true;

    // 开启异步线程执行定时发送逻辑
    worker_ = std::thread([this]() {
      logger->info("数据源线程已启动.");
      while (running_) {
        // send(port_name, data): 向指定端口发送数据
        // FINS 内部会自动给这条数据打上当前的时间戳（AcqTime）
        this->send("str", msg);
        
        // 控制发送频率（约 1kHz）
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    });
  }

  /**
   * pause(): 暂停/停止逻辑
   * 必须确保在这里回收所有自建的线程。
   */
  void pause() override {
    running_ = false;
    if (worker_.joinable()) {
      worker_.join();
    }
    logger->info("Source 已暂停.");
  }

  void reset() override { logger->info("Source 重置."); }

private:
  std::thread worker_;
  std::atomic<bool> running_{false};
  const std::string msg = "Hello, World! from fins";
};

// 导出节点，使之在编译为 .so 后能被 NodeLib 识别
EXPORT_NODE(HelloWorldSource)


/**
 * HelloWorldPrinter: 打印节点（消费者）
 * 演示如何接收数据并测量框架的传输延迟。
 */
class HelloWorldPrinter : public fins::Node {
public:
  HelloWorldPrinter() = default;

  void define() override {
    set_name("HelloWorldPrinter");
    set_description("打印收到的字符串");
    set_category("Tutorials");

    // register_input<T>(port_name, callback): 注册输入端口并绑定回调
    // 当有数据通过 Pipe 到达该端口时，框架的 ThreadManager 会自动调用绑定的成员函数。
    register_input<std::string>("str", &HelloWorldPrinter::receive_msg);
  }

  void initialize() override { logger->info("Printer 初始化."); }

  /**
   * run()/pause(): 即使没有内部线程，也需要注册这些方法。
   */
  void run() override { logger->info("Printer 进入运行状态."); }

  void pause() override { logger->info("Printer 进入暂停状态."); }

  void reset() override { logger->info("Printer 重置."); }

  /**
   * receive_msg: 回调函数
   * @param msg 传递的数据内容
   * @param acq_time 数据产生时的时间戳（由 Source 节点的 send 自动生成）
   */
  void receive_msg(const std::string &msg, fins::AcqTime acq_time) {
    static size_t count = 0;
    static uint64_t total_latency = 0;

    // fins::latency_us(acq_time): 计算从数据产生到此刻进入回调的耗时
    total_latency += fins::latency_us(acq_time);

    if (++count >= 1000) {
      // 每一千次计算一次平均延迟，展示 50us 左右的高性能表现
      logger->info("收到消息: {}. 平均框架延迟: {} us",
                   msg, (total_latency / 1000.0));
      count = 0;
      total_latency = 0;
    }
  }
};

EXPORT_NODE(HelloWorldPrinter)