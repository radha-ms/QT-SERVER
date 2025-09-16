///**
//  ******************************************************************************
//  * @file    Queue_Manager.cpp
//  * @author  karthikeya.sr
//  * @version V1.0
//  * @date    02-May-2025
//  * @brief   Implementation of the Queue_Manager class.
//  *
//  * @details This file defines the implementation of the `Queue_Manager` class,
//  *          which manages multiple thread-safe queues used across the DACS_Server_Application application.
//  *          This module is responsible for:
//  *          1. Dynamically creating or retrieving thread-safe queues based on a unique key.
//  *          2. Supporting queues for different data types (e.g., ANALOG_DATA, DIGITAL_DATA) using templates.
//  *          3. Internally storing queues as base class pointers to allow type flexibility.
//  *          4. Providing type-safe access to queues by dynamically casting when retrieving.
//  *          5. Allowing initialization of predefined common queues for application modules.
//  *
//  * @note    Each queue is managed using `QSharedPointer` to ensure memory safety and thread-safe operations
//  *          when accessing or modifying the queues across multiple threads.
//  *
//  ******************************************************************************
//  */

//#include "Queue_Manager.h"

///**
// * @brief   Creates or retrieves a queue associated with a given key and data type T.
// *
// * @tparam  T : Type of data the queue will hold (e.g., ANALOG_DATA, DIGITAL_DATA).
// * @param   key : A QString key identifying the queue.
// * @return  QSharedPointer<ThreadSafeQueue<T>> : A shared pointer to the thread-safe queue of type T.
// *
// * @details
// * - If the queue corresponding to the key does not exist, a new ThreadSafeQueue<T> is created.
// * - The queue is stored internally as a BaseQueue, and dynamically cast back to ThreadSafeQueue<T> when accessed.
// * - Ensures type safety and avoids duplicate queues with the same key.
// */
//template <typename T>
//QSharedPointer<ThreadSafeQueue<T>> Queue_Manager::getOrCreateQueue(const QString &key) {
//    if (!queues.contains(key)) {
//        // If the queue does not exist, create it and add it to the map
//        queues[key] = QSharedPointer<BaseQueue>(new ThreadSafeQueue<T>());
//    }

//    // Return the queue by casting it to the correct specific type
//    return queues[key].dynamicCast<ThreadSafeQueue<T>>();
//}

///**
// * @brief   Initializes predefined queues for common data types.
// *
// * @details
// * - This function sets up frequently used queues during system startup.
// * - It creates queues for types like ANALOG_DATA, DIGITAL_DATA.
// * - Helps avoid accidental missing queue creation during runtime.
// *
// * Example queues initialized:
// * - "analog" queue for ANALOG_DATA
// * - "digital" queue for DIGITAL_DATA
// */
//void Queue_Manager::initializeQueues() {
//    getOrCreateQueue<ANALOG_DATA>("analog");     // Queue for analog channel data
//    getOrCreateQueue<DIGITAL_DATA>("digital");   // Queue for digital channel data
//}



//template<typename T>
//class BlockingQueue {
//public:
//    void enqueue(const T& item) {
//        std::unique_lock<std::mutex> lock(mutex_);
//        queue_.push(item);
//        cond_.notify_one();
//    }

//    T dequeue() {
//        std::unique_lock<std::mutex> lock(mutex_);
//        cond_.wait(lock, [this] { return !queue_.empty(); });

//        T item = queue_.front();
//        queue_.pop();
//        return item;
//    }

//    bool try_dequeue(T& item) {
//        std::unique_lock<std::mutex> lock(mutex_);
//        if (queue_.empty()) return false;
//        item = queue_.front();
//        queue_.pop();
//        return true;
//    }

//private:
//    std::queue<T> queue_;
//    std::mutex mutex_;
//    std::condition_variable cond_;
//};
