#ifndef LOCKFREEQUEUE_H
#define LOCKFREEQUEUE_H

#include <iostream>
#include <atomic>
#include <memory>

#include <QObject>
#include <QMap>
#include <QSharedPointer>
#include <QString>
#include <QQueue>
#include <QMutex>
#include <QWaitCondition>
#include "Data_Structure.h"

template <typename T>
class LockFreeQueue
{
private:
    struct dataQueue
    {
        std::shared_ptr<T> data;
        std::atomic<dataQueue*> next;

        dataQueue() : next(nullptr) {}
        dataQueue(T val) : data(std::make_shared<T>(std::move(val))), next(nullptr) {}
    };

    std::atomic<dataQueue*> head;
    std::atomic<dataQueue*> tail;

public:
    LockFreeQueue()
    {
        dataQueue* dummyNode = new dataQueue();  // Dummy node
        head.store(dummyNode);
        tail.store(dummyNode);
    }

    ~LockFreeQueue()
    {
        T temp;
        while (deQueue(temp)) {
            // clean remaining nodes
        }

        dataQueue* final = head.load();
        delete final;
    }

    void enQueue(T value)
    {
        dataQueue* new_node = new dataQueue(std::move(value));

        while (true)
        {
            dataQueue* old_tail = tail.load(std::memory_order_acquire);
            dataQueue* next = old_tail->next.load(std::memory_order_acquire);

            if (next == nullptr)
            {
                if (old_tail->next.compare_exchange_weak(next, new_node))
                {
                    tail.compare_exchange_strong(old_tail, new_node); // Advance tail
                    return;
                }
            }
            else
            {
                tail.compare_exchange_weak(old_tail, next); // Help move tail forward
            }
        }
    }

    bool deQueue(T& value)
    {
        while (true)
        {
            dataQueue* old_head = head.load(std::memory_order_acquire);
            dataQueue* next = old_head->next.load(std::memory_order_acquire);

            if (next == nullptr)
                return false; // Queue is empty

            if (head.compare_exchange_weak(old_head, next))
            {
                value = std::move(*next->data);
                delete old_head; // Delete old dummy
                return true;
            }
        }
    }
};

#endif // LOCKFREEQUEUE_H

















//template<typename T>
//class Queue_Manager {
//public:
//    Queue_Manager() = default;

//    void enqueue(const T& item) {
//        QMutexLocker locker(&mutex);
//        queue.enqueue(item);
//        notEmpty.wakeOne();
//    }

//    T dequeue() {
//        QMutexLocker locker(&mutex);
//        while (queue.isEmpty()) {
//            notEmpty.wait(&mutex);
//        }
//        return queue.dequeue();
//    }

//    bool isEmpty() const {
//        QMutexLocker locker(&mutex);
//        return queue.isEmpty();
//    }

//    int size() const {
//        QMutexLocker locker(&mutex);
//        return queue.size();
//    }

//private:
//    mutable QMutex mutex;
//    QWaitCondition notEmpty;
//    QQueue<T> queue;
//    QSharedPointer<Queue_Manager<T>> queus;

//};

//  QUEUE_MANAGER_H








