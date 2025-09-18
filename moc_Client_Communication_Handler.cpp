/****************************************************************************
** Meta object code from reading C++ file 'Client_Communication_Handler.h'
**
** Created by: The Qt Meta Object Compiler version 67 (Qt 5.12.4)
**
** WARNING! All changes made in this file will be lost!
*****************************************************************************/

#include "Client_Communication_Handler.h"
#include <QtCore/qbytearray.h>
#include <QtCore/qmetatype.h>
#if !defined(Q_MOC_OUTPUT_REVISION)
#error "The header file 'Client_Communication_Handler.h' doesn't include <QObject>."
#elif Q_MOC_OUTPUT_REVISION != 67
#error "This file was generated using the moc from 5.12.4. It"
#error "cannot be used with the include files from this version of Qt."
#error "(The moc has changed too much.)"
#endif

QT_BEGIN_MOC_NAMESPACE
QT_WARNING_PUSH
QT_WARNING_DISABLE_DEPRECATED
struct qt_meta_stringdata_Client_Communication_Handler_t {
    QByteArrayData data[11];
    char stringdata0[162];
};
#define QT_MOC_LITERAL(idx, ofs, len) \
    Q_STATIC_BYTE_ARRAY_DATA_HEADER_INITIALIZER_WITH_OFFSET(len, \
    qptrdiff(offsetof(qt_meta_stringdata_Client_Communication_Handler_t, stringdata0) + ofs \
        - idx * sizeof(QByteArrayData)) \
    )
static const qt_meta_stringdata_Client_Communication_Handler_t qt_meta_stringdata_Client_Communication_Handler = {
    {
QT_MOC_LITERAL(0, 0, 28), // "Client_Communication_Handler"
QT_MOC_LITERAL(1, 29, 21), // "receiveDataFromSocket"
QT_MOC_LITERAL(2, 51, 0), // ""
QT_MOC_LITERAL(3, 52, 7), // "Command"
QT_MOC_LITERAL(4, 60, 13), // "newConnection"
QT_MOC_LITERAL(5, 74, 18), // "clientDisconnected"
QT_MOC_LITERAL(6, 93, 14), // "readClientData"
QT_MOC_LITERAL(7, 108, 20), // "UpdateSocketSendData"
QT_MOC_LITERAL(8, 129, 6), // "STATUS"
QT_MOC_LITERAL(9, 136, 20), // "sendDataToAllClients"
QT_MOC_LITERAL(10, 157, 4) // "data"

    },
    "Client_Communication_Handler\0"
    "receiveDataFromSocket\0\0Command\0"
    "newConnection\0clientDisconnected\0"
    "readClientData\0UpdateSocketSendData\0"
    "STATUS\0sendDataToAllClients\0data"
};
#undef QT_MOC_LITERAL

static const uint qt_meta_data_Client_Communication_Handler[] = {

 // content:
       8,       // revision
       0,       // classname
       0,    0, // classinfo
       6,   14, // methods
       0,    0, // properties
       0,    0, // enums/sets
       0,    0, // constructors
       0,       // flags
       1,       // signalCount

 // signals: name, argc, parameters, tag, flags
       1,    1,   44,    2, 0x06 /* Public */,

 // slots: name, argc, parameters, tag, flags
       4,    0,   47,    2, 0x08 /* Private */,
       5,    0,   48,    2, 0x08 /* Private */,
       6,    0,   49,    2, 0x08 /* Private */,
       7,    1,   50,    2, 0x08 /* Private */,
       9,    1,   53,    2, 0x08 /* Private */,

 // signals: parameters
    QMetaType::Void, QMetaType::Int,    3,

 // slots: parameters
    QMetaType::Void,
    QMetaType::Void,
    QMetaType::Void,
    QMetaType::Void, 0x80000000 | 8,    3,
    QMetaType::Void, 0x80000000 | 8,   10,

       0        // eod
};

void Client_Communication_Handler::qt_static_metacall(QObject *_o, QMetaObject::Call _c, int _id, void **_a)
{
    if (_c == QMetaObject::InvokeMetaMethod) {
        auto *_t = static_cast<Client_Communication_Handler *>(_o);
        Q_UNUSED(_t)
        switch (_id) {
        case 0: _t->receiveDataFromSocket((*reinterpret_cast< int(*)>(_a[1]))); break;
        case 1: _t->newConnection(); break;
        case 2: _t->clientDisconnected(); break;
        case 3: _t->readClientData(); break;
        case 4: _t->UpdateSocketSendData((*reinterpret_cast< const STATUS(*)>(_a[1]))); break;
        case 5: _t->sendDataToAllClients((*reinterpret_cast< STATUS(*)>(_a[1]))); break;
        default: ;
        }
    } else if (_c == QMetaObject::RegisterMethodArgumentMetaType) {
        switch (_id) {
        default: *reinterpret_cast<int*>(_a[0]) = -1; break;
        case 4:
            switch (*reinterpret_cast<int*>(_a[1])) {
            default: *reinterpret_cast<int*>(_a[0]) = -1; break;
            case 0:
                *reinterpret_cast<int*>(_a[0]) = qRegisterMetaType< STATUS >(); break;
            }
            break;
        case 5:
            switch (*reinterpret_cast<int*>(_a[1])) {
            default: *reinterpret_cast<int*>(_a[0]) = -1; break;
            case 0:
                *reinterpret_cast<int*>(_a[0]) = qRegisterMetaType< STATUS >(); break;
            }
            break;
        }
    } else if (_c == QMetaObject::IndexOfMethod) {
        int *result = reinterpret_cast<int *>(_a[0]);
        {
            using _t = void (Client_Communication_Handler::*)(int );
            if (*reinterpret_cast<_t *>(_a[1]) == static_cast<_t>(&Client_Communication_Handler::receiveDataFromSocket)) {
                *result = 0;
                return;
            }
        }
    }
}

QT_INIT_METAOBJECT const QMetaObject Client_Communication_Handler::staticMetaObject = { {
    &QTcpServer::staticMetaObject,
    qt_meta_stringdata_Client_Communication_Handler.data,
    qt_meta_data_Client_Communication_Handler,
    qt_static_metacall,
    nullptr,
    nullptr
} };


const QMetaObject *Client_Communication_Handler::metaObject() const
{
    return QObject::d_ptr->metaObject ? QObject::d_ptr->dynamicMetaObject() : &staticMetaObject;
}

void *Client_Communication_Handler::qt_metacast(const char *_clname)
{
    if (!_clname) return nullptr;
    if (!strcmp(_clname, qt_meta_stringdata_Client_Communication_Handler.stringdata0))
        return static_cast<void*>(this);
    return QTcpServer::qt_metacast(_clname);
}

int Client_Communication_Handler::qt_metacall(QMetaObject::Call _c, int _id, void **_a)
{
    _id = QTcpServer::qt_metacall(_c, _id, _a);
    if (_id < 0)
        return _id;
    if (_c == QMetaObject::InvokeMetaMethod) {
        if (_id < 6)
            qt_static_metacall(this, _c, _id, _a);
        _id -= 6;
    } else if (_c == QMetaObject::RegisterMethodArgumentMetaType) {
        if (_id < 6)
            qt_static_metacall(this, _c, _id, _a);
        _id -= 6;
    }
    return _id;
}

// SIGNAL 0
void Client_Communication_Handler::receiveDataFromSocket(int _t1)
{
    void *_a[] = { nullptr, const_cast<void*>(reinterpret_cast<const void*>(&_t1)) };
    QMetaObject::activate(this, &staticMetaObject, 0, _a);
}
QT_WARNING_POP
QT_END_MOC_NAMESPACE
