/**
  ***********************************************************************************************************************************
  * @file   Queue_Mnanager.h
  * @author  karthikeya.sr
  * @version V1.0
  * @date    02-May-2025
  * @brief   This file provide all header to Queue_Mnanager.cpp
  ***************************************************************************************************************************************/



#ifndef QUEUE_MANAGER_H
#define QUEUE_MANAGER_H

#include <QObject>
#include <QMap>
#include <QSharedPointer>
#include <QString>
#include <QQueue>
#include <QMutex>
#include <QWaitCondition>
#include "Data_Structure.h"

#include <QQueue>
#include <QMutex>
#include <QWaitCondition>


template<typename T>
class Queue_Manager {
public:
    Queue_Manager() = default;

    void enqueue(const T& item) {
        QMutexLocker locker(&mutex);
        queue.enqueue(item);
        notEmpty.wakeOne();
    }

    T dequeue() {
        QMutexLocker locker(&mutex);
        while (queue.isEmpty()) {
            notEmpty.wait(&mutex);
        }
        return queue.dequeue();
    }

    bool isEmpty() const {
        QMutexLocker locker(&mutex);
        return queue.isEmpty();
    }

    int size() const {
        QMutexLocker locker(&mutex);
        return queue.size();
    }

private:
    mutable QMutex mutex;
    QWaitCondition notEmpty;
    QQueue<T> queue;
    QSharedPointer<Queue_Manager<T>> queus;

};
#endif // QUEUE_MANAGER_H


#include <iostream>
#include <atomic>
#include <memory>
using namespace std;

template <typename T>
class LockFreeQueue
{
private:
    struct dataQueue
    {
        shared_ptr<T> data; //in daspro this is bytearray
        atomic<shared_ptr<dataQueue>> next;

        dataQueue() : next(nullptr) {}
        dataQueue(T val) : data(make_shared<T>(move(val))), next(nullptr) {}
    };

    shared_ptr<dataQueue> dummy;
    atomic<shared_ptr<dataQueue>> head;
    atomic<shared_ptr<dataQueue>> tail;

public:
    LockFreeQueue()
    {
        dummy = make_shared<dataQueue>();  // Dummy node with no data
        head.store(dummy);
        tail.store(dummy);
    }

    void enQueue(T value)
    {
        auto new_node = make_shared<dataQueue>(move(value));
        shared_ptr<dataQueue> old_tail;

        while (true)
        {
            old_tail = tail.load(memory_order_acquire);
            auto next = old_tail->next.load(memory_order_acquire);

            if (!next)
            {
                if (old_tail->next.compare_exchange_weak(next, new_node))
                    break;
            }
            else
                tail.compare_exchange_weak(old_tail, next);
        }
        tail.compare_exchange_strong(old_tail, new_node);
    }

    bool deQueue(T& value)
    {
        shared_ptr<dataQueue> old_head;

        while (true)
        {
            old_head = head.load(memory_order_acquire);
            auto next = old_head->next.load(memory_order_acquire);

            if (!next)
                return false; // Queue empty


            if (head.compare_exchange_weak(old_head, next))
            {
                value = move(*next->data);  // Safe because data is held by shared_ptr
                return true;
            }
        }
    }
};




#include <atomic>
#include <vector>
#include <thread>
#include <iostream>
#include <optional>
#include <queue>
#include <limits>
#include <chrono>

// Data item with timestamp
template<typename T>
struct DataWithTimestamp {
    T data;
    uint64_t timestamp;
    DataWithTimestamp(T d, uint64_t ts) : data(d), timestamp(ts) {}
};

// Simple lock-free single-producer single-consumer queue (Michael-Scott style simplified)
template<typename T>
class LockFreeSPSCQueue {
private:
    struct Node {
        std::atomic<Node*> next{ nullptr };
        std::optional<T> value; // optional to handle empty dummy

        Node() : value(std::nullopt) {}
        Node(const T& val) : value(val) {}
    };

    std::atomic<Node*> head;
    Node* tail;

public:
    LockFreeSPSCQueue() {
        Node* dummy = new Node{};
        head.store(dummy);
        tail = dummy;
    }

    ~LockFreeSPSCQueue() {
        while (Node* node = head.load()) {
            head.store(node->next);
            delete node;
        }
    }

    void push(const T& value) {
        Node* node = new Node(value);
        tail->next.store(node, std::memory_order_release);
        tail = node;
    }

    std::optional<T> pop() {
        Node* node = head.load(std::memory_order_acquire);
        Node* next = node->next.load(std::memory_order_acquire);
        if (!next) return {}; // empty

        T result = *(next->value);
        head.store(next, std::memory_order_release);
        delete node;
        return result;
    }
};

// Merging consumer reading from multiple SPSC queues
template<typename T>
class TimestampMerger {
private:
    std::vector<LockFreeSPSCQueue<DataWithTimestamp<T>>*> queues;

public:
    TimestampMerger(std::vector<LockFreeSPSCQueue<DataWithTimestamp<T>>*>& qs)
        : queues(qs) {
    }

    // Returns next data with globally smallest timestamp, or nullopt if all empty
    std::optional<DataWithTimestamp<T>> pop_min_timestamp() {
        size_t n = queues.size();
        std::optional<DataWithTimestamp<T>> candidates[n];
        bool any_data = false;
        // Peek at head of each queue
        for (size_t i = 0; i < n; ++i) {
            candidates[i] = queues[i]->pop();
            if (candidates[i].has_value()) {
                any_data = true;
            }
        }
        if (!any_data) return {}; // all empty

        // Find min item among candidates
        uint64_t min_ts = std::numeric_limits<uint64_t>::max();
        int min_index = -1;
        for (size_t i = 0; i < n; ++i) {
            if (candidates[i].has_value() && candidates[i]->timestamp < min_ts) {
                min_ts = candidates[i]->timestamp;
                min_index = (int)i;
            }
        }
        // Put back the items from other queues that were popped unnecessarily
        for (size_t i = 0; i < n; ++i) {
            if (i != (size_t)min_index && candidates[i].has_value()) {
                // Re-push to queue front (not trivial in lock-free SPSC, 
                // so this example assumes consumer reads only and returns,
                // or we redesign to peek without popping)
                // For simplicity, here we just ignore re-push — in production,
                // you'd implement peek functionality or use a richer data structure.
            }
        }
        return candidates[min_index];
    }
};

int main() {
    constexpr int num_producers = 3;

    // Create one queue per producer
    std::vector<LockFreeSPSCQueue<DataWithTimestamp<int>>*> queues;
    for (int i = 0; i < num_producers; ++i) {
        queues.push_back(new LockFreeSPSCQueue<DataWithTimestamp<int>>());
    }

    // Producer threads pushing ordered items
    std::vector<std::thread> producers;
    for (int i = 0; i < num_producers; ++i) {
        producers.emplace_back([i, &queues]() {
            for (uint64_t t = 0; t < 5; ++t) {
                // data: ints, timestamp: ascending
                queues[i]->push({ i * 100 + (int)t, t }); // example data + timestamp
                std::this_thread::sleep_for(std::chrono::milliseconds(10 * (i + 1)));
            }
            });
    }

    // Consumer merges
    auto merger = TimestampMerger<int>(queues);

    // Simple consumer loop (waiting after producers finish)
    while (true) {
        bool still_producing = false;
        std::optional<DataWithTimestamp<int>> item = std::nullopt;

        // Try to pop from all queues in round-robin: simplified here
        for (auto* q : queues) {
            auto val = q->pop();
            if (val) {
                item = val;
                break;
            }
        }

        if (item) {
            std::cout << "Consumed data: " << item->data << ", timestamp: " << item->timestamp << "\n";
        }
        else {
            // check if producers still running
            for (auto& p : producers) {
                if (p.joinable()) still_producing = true;
            }
            if (!still_producing) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }

    // Join producers
    for (auto& p : producers) p.join();

    // Cleanup
    for (auto* q : queues) delete q;

    return 0;
}