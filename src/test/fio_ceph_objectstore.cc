// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 *  Ceph ObjectStore engine
 *
 * IO engine using Ceph's ObjectStore class to test low-level performance of
 * Ceph OSDs.
 *
 */

#include "os/ObjectStore.h"
#include "global/global_init.h"
#include "common/errno.h"

#include <fio.h>

struct ceph_os_data {
  std::vector<io_u*> aio_events;
  std::unique_ptr<ObjectStore> fs;
  ObjectStore::Sequencer sequencer;

  ceph_os_data(int iodepth)
    : aio_events(iodepth), fs(nullptr), sequencer("fio") {}
};

struct ceph_os_options {
  thread_data *td;
  char *objectstore;
  char *filestore_debug;
  char *filestore_journal;
};

// initialize the options in a function because g++ reports:
//   sorry, unimplemented: non-trivial designated initializers not supported
static fio_option* init_options() {
  static fio_option options[] = {{},{},{},{}};

  options[0].name     = "objectstore";
  options[0].lname    = "ceph objectstore type";
  options[0].type     = FIO_OPT_STR_STORE;
  options[0].help     = "Type of ObjectStore to create";
  options[0].off1     = offsetof(ceph_os_options, objectstore);
  options[0].def      = "filestore";
  options[0].category = FIO_OPT_C_ENGINE;
  options[0].group    = FIO_OPT_G_RBD;

  options[1].name     = "filestore_debug";
  options[1].lname    = "ceph filestore debug level";
  options[1].type     = FIO_OPT_STR_STORE;
  options[1].help     = "Debug level for ceph filestore log output";
  options[1].off1     = offsetof(ceph_os_options, filestore_debug);
  options[1].category = FIO_OPT_C_ENGINE;
  options[1].group    = FIO_OPT_G_RBD;

  options[2].name     = "filestore_journal";
  options[2].lname    = "ceph filestore journal path";
  options[2].type     = FIO_OPT_STR_STORE;
  options[2].help     = "Path for a temporary journal file";
  options[2].off1     = offsetof(ceph_os_options, filestore_journal);
  options[2].def      = "";
  options[2].category = FIO_OPT_C_ENGINE;
  options[2].group    = FIO_OPT_G_RBD;

  return options;
};


static io_u* fio_ceph_os_event(thread_data *td, int event)
{
  auto data = static_cast<ceph_os_data*>(td->io_ops->data);
  return data->aio_events[event];
}

static int fio_ceph_os_getevents(thread_data *td, unsigned int min,
                                 unsigned int max, const timespec *t)
{
  auto data = static_cast<ceph_os_data*>(td->io_ops->data);
  unsigned int events = 0;
  io_u *u;
  unsigned int i;

  do {
    io_u_qiter(&td->io_u_all, u, i) {
      if (!(u->flags & IO_U_F_FLIGHT))
        continue;

      if (u->engine_data) {
        u->engine_data = nullptr;
        data->aio_events[events] = u;
        events++;
      }
    }
    if (events < min)
      usleep(100);
    else
      break;

  } while (1);

  return events;
}

struct OnApplied : public Context {
  io_u *u;
  ObjectStore::Transaction *t;
  OnApplied(io_u *u, ObjectStore::Transaction *t) : u(u), t(t) {}
  void finish(int r) {
    u->engine_data = reinterpret_cast<void*>(1ull);
    delete t;
  }
};

static int fio_ceph_os_queue(thread_data *td, io_u *u)
{
  fio_ro_check(td, u);

  bufferlist bl;
  bl.push_back(buffer::create_static(u->xfer_buflen,
                                     static_cast<char*>(u->xfer_buf)));

  spg_t pg;
  ghobject_t oid{hobject_t{sobject_t{u->file->file_name, CEPH_NOSNAP}}};

  auto data = static_cast<ceph_os_data*>(td->io_ops->data);
  auto &fs = data->fs;

  if (u->ddir == DDIR_WRITE) {
    auto t = new ObjectStore::Transaction;
    t->write(coll_t(pg), oid, u->offset, u->xfer_buflen, bl);
    fs->queue_transaction(&data->sequencer, t, new OnApplied(u, t), nullptr);
    return FIO_Q_QUEUED;
  }

  if (u->ddir == DDIR_READ) {
    int r = fs->read(coll_t(pg), oid, u->offset, u->xfer_buflen, bl);
    if (r < 0) {
      u->error = r;
      td_verror(td, u->error, "xfer");
    } else {
      u->resid = u->xfer_buflen - r;
    }
    return FIO_Q_COMPLETED;
  }

  cout << "WARNING: Only DDIR_READ and DDIR_WRITE are supported!" << std::endl;
  u->error = -EINVAL;
  td_verror(td, u->error, "xfer");
  return FIO_Q_COMPLETED;
}

static int fio_ceph_os_init(thread_data *td)
{
  vector<const char*> args;
  global_init(NULL, args, CEPH_ENTITY_TYPE_OSD, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  const auto o = static_cast<ceph_os_options*>(td->eo);

  // enable experimental features
  if (strcmp(o->objectstore, "newstore") == 0)
    g_conf->set_val("enable_experimental_unrecoverable_data_corrupting_features",
                    "newstore rocksdb");
  else if (strcmp(o->objectstore, "keyvaluestore") == 0)
    g_conf->set_val("enable_experimental_unrecoverable_data_corrupting_features",
                    "keyvaluestore");

  if (o->filestore_debug)
    g_conf->set_val("debug_filestore", o->filestore_debug);
  g_conf->apply_changes(NULL);

  std::unique_ptr<ObjectStore> fs(ObjectStore::create(
      g_ceph_context, o->objectstore, td->o.directory,
      o->filestore_journal ? o->filestore_journal : ""));
  if (!fs) {
    cout << "bad objectstore type " << o->objectstore << std::endl;
    return 1;
  }
  int r = fs->mkfs();
  if (r < 0) {
    std::cerr << "mkfs failed with " << cpp_strerror(-r) << std::endl;
    return 1;
  }
  r = fs->mount();
  if (r < 0) {
    std::cerr << "mount failed with " << cpp_strerror(-r) << std::endl;
    return 1;
  }

  auto data = static_cast<ceph_os_data*>(td->io_ops->data);

  spg_t pg;
  auto coll = coll_t{pg};
  if (!fs->collection_exists(coll)) {
    ObjectStore::Transaction t;
    t.create_collection(coll, 0);
    fs->apply_transaction(&data->sequencer, t);
  }

  std::swap(fs, data->fs);
  return 0;
}

static void fio_ceph_os_cleanup(thread_data *td)
{
  auto data = static_cast<ceph_os_data*>(td->io_ops->data);
  if (data && data->fs)
    data->fs->umount();
  delete data;
}

static int fio_ceph_os_setup(thread_data *td)
{
  /* allocate engine specific structure to deal with libceph_os. */
  td->io_ops->data = new ceph_os_data(td->o.iodepth);
  return 0;
}

static int fio_ceph_os_open(thread_data *td, fio_file *f)
{
  spg_t pg;
  coll_t coll{pg};
  ghobject_t oid{hobject_t{sobject_t{f->file_name, CEPH_NOSNAP}}};

  ObjectStore::Transaction t;
  t.touch(coll, oid);
  t.truncate(coll, oid, f->real_file_size);

  auto data = static_cast<ceph_os_data*>(td->io_ops->data);
  return data->fs->apply_transaction(&data->sequencer, t);
}

static int fio_ceph_os_close(thread_data *td, fio_file *f)
{
  spg_t pg;
  coll_t coll{pg};
  ghobject_t oid{hobject_t{sobject_t{f->file_name, CEPH_NOSNAP}}};

  ObjectStore::Transaction t;
  t.remove(coll, oid);

  auto data = static_cast<ceph_os_data*>(td->io_ops->data);
  return data->fs->apply_transaction(&data->sequencer, t);
}

static void fio_ceph_os_io_u_free(thread_data *td, io_u *u)
{
  u->engine_data = nullptr;
}

static int fio_ceph_os_io_u_init(thread_data *td, io_u *u)
{
  u->engine_data = nullptr;
  return 0;
}

extern "C" {
  void get_ioengine(struct ioengine_ops **ioengine_ptr) {
    struct ioengine_ops *ioengine;
    *ioengine_ptr = (struct ioengine_ops *) calloc(sizeof(struct ioengine_ops), 1);
    ioengine = *ioengine_ptr;

    strcpy(ioengine->name, "cephobjectstore");
    ioengine->version        = FIO_IOOPS_VERSION;
    ioengine->setup          = fio_ceph_os_setup;
    ioengine->init           = fio_ceph_os_init;
    ioengine->queue          = fio_ceph_os_queue;
    ioengine->getevents      = fio_ceph_os_getevents;
    ioengine->event          = fio_ceph_os_event;
    ioengine->cleanup        = fio_ceph_os_cleanup;
    ioengine->open_file      = fio_ceph_os_open;
    ioengine->close_file     = fio_ceph_os_close;
    ioengine->io_u_init      = fio_ceph_os_io_u_init;
    ioengine->io_u_free      = fio_ceph_os_io_u_free;
    ioengine->options        = init_options();
    ioengine->option_struct_size = sizeof(struct ceph_os_options);
  }
}
