#!/usr/bin/env python
import logging
import pymongo
from pymongo import MongoClient
import time
import subprocess
import random
import os
import datetime
import threading
import Queue

""" Create consistent backup of your mongodb sharded cluster.
    This basically follows mongodb shard backup procedure described in mongodb
    documentation (http://bit.ly/11uNuYa). It:
        - stops cluster balancer;
        - stops one of configuration servers to prevent metadata changes;
        - backs up configuration database;
        - locks all shards. If your shards are replicasets, only one of the
          secondary servers will be locked;
        - creates LVM snapshots on the servers;
        - unlocks the shards;
        - starts the configuration server and enables the balancer.
    This ensures that consistent cluster data is present on the servers. You
    will still have to copy that data to the backup medium of your choice and
    remove LVM snapshots.
    This expects that the machine you run this on has a passwordless ssh
    access to configuration servers and shards.
"""

logging.basicConfig(format="%(asctime)-15s %(message)s", level=logging.INFO)
configReplica = 0

class BackupAbortedException(Exception): pass


class BackupMongo:

    """ Superclass for all mongo servers """

    def __init__(self, host):
        self.host = host
        logging.info("Initializing %s(%s)" % (self.__class__, self.host))
        self.client = MongoClient(self.host)


class SSHAble():

    """ Superclass for all ssh-able objects """

    def run(self, command, timeout=3600, capture_output=False):
        cmd = ("timeout %d ssh -o StrictHostKeyChecking=no"
               " -o ConnectTimeout=60 -o ServerAliveInterval=20 -l root"
               " %s '%s'" % (timeout, self.host.split(':')[0], command))
        logging.info("> %s" % cmd)
        if capture_output:
            return subprocess.check_output(cmd, shell=True)
        else:
            return subprocess.call(cmd, shell=True)

class BackupMongos(BackupMongo):

    """ Mongos instance. We will use it to stop/start balancer and wait for
        any locks.
    """

    def get_shards(self):
        shards = []
        for shard in self.client['config']['shards'].find():
            if '/' in shard['host']:
                # This shard is a replicaset. Connect to it and find a healthy
                # secondary host with minimal replication lag
                hosts = shard['host'].split('/')[1]
                with MongoClient(hosts.split(',')) as connection:
                    rs = connection['admin'].command("replSetGetStatus")
                    good_secondaries = [member for member in rs['members']
                        if member['state'] == 2 and int(member['health'])]
                    if len(good_secondaries):
                        best = sorted(good_secondaries,
                                      key=lambda x: x['optimeDate'],
                                      reverse=True)[0]
                        shards.append(best['name'])
                    else:
                        # no healthy secondaries found, try to find the master
                        master = [member for member in rs['members']
                            if member['state'] == 1][0]
                        shards.append(master['name'])
            else:
                # standalone server rather than a replicaset
                shards.append(shard['host'])
        return shards

    def get_locks(self):
        return [lock
            for lock in self.client['config']['locks'].find({"state": 2})]

    def balancer_stopped(self):
        return self.client['config']['settings'].find({
                                                      "_id": "balancer"
                                                      })[0]["stopped"]

    def stop_balancer(self):
        logging.info("Stopping balancer")
        self.client['config']['settings'].update(
                                                 {"_id":"balancer"}, {"$set": {"stopped": True}})
        if not self.balancer_stopped():
            raise Exception("Could not stop balancer")

    def start_balancer(self):
        logging.info("Starting balancer")
        self.client['config']['settings'].update(
                                                 {"_id":"balancer"}, {"$set": {"stopped": False}})
        if self.balancer_stopped():
            raise Exception("Could not start balancer")
        
#        self.client.admin.command('ismaster')

    def get_config_servers(self):
        global configReplica
        cmd_line_opts = self.client['admin'].command('getCmdLineOpts')
        serverslist = cmd_line_opts['parsed']['sharding']['configDB'].split('/')[1]
        servers = serverslist.split(',')
        print servers
        found = 0
        for server in servers:
            print server;
            configserver = BackupConfig(server)
            if configserver.client.admin.command('ismaster')['ismaster'] == False:
                found = 1
                print configserver
                break
        print server
        configReplica = configserver
        return server


class BackupShard(BackupMongo):

    """ A specific shard server. We will be locking/unlocking these for backup
        data to be consistent.
    """

    def lock(self, errors=Queue.Queue()):
        logging.info("Locking shard %s" % self.host)
        self.client.fsync(lock=True)
        if self.is_locked:
            logging.info("Locked shard %s" % self.host)
        else:
            err = "Cannot lock shard %s" % self.host
            logging.error(err)
            errors.put(err)

    def unlock(self):
        logging.info("Unlocking shard %s" % self.host)
        self.client.unlock()
        if self.is_locked():
            raise Exception("Cannot unlock shard %s" % self.host)

    def is_locked(self):
        return self.client.is_locked
   
    def is_master(self):
        return self.client.admin.command('ismaster')

class BackupConfig(BackupMongo):

    """ locking/unlocking config server for backup
        data to be consistent.
    """

    def lock(self, errors=Queue.Queue()):
        logging.info("Locking Config server %s" % self.host)
        self.client.fsync(lock=True)
        if self.is_locked:
            logging.info("Locked Config server %s" % self.host)
        else:
            err = "Cannot lock Config server %s" % self.host
            logging.error(err)
            errors.put(err)

    def unlock(self):
        logging.info("Unlocking Config server %s" % self.host)
        self.client.unlock()
        if self.is_locked():
            raise Exception("Cannot unlock Config server %s" % self.host)

    def is_locked(self):
        return self.client.is_locked
   
    def is_master(self):
        return self.client.admin.command('ismaster')
    
    

class BackupConfigServer(SSHAble):

    """ Configuration server that is going to be stopped to prevent any
        metadata changes to the cluster. Also, it will be used to backup
        `config` database.
    """

    def __init__(self, host, backup_path):
        self.host = host
        self.backup_path = backup_path
        logging.info("Initializing BackupConfigServer(%s)" % self.host)
        if not self.is_running():
            raise BackupAbortedException("MongoDB is not running on %s" %
                                         self.host)

    def is_running(self):
        check_mongod = self.run("service mongod status")
        return check_mongod == 0

    def stop(self):
        logging.info("Stopping mongo configuration server on %s" % self.host)
        stop_mongod = self.run("service mongod start")
        time.sleep(3)
#        if self.is_running():
#            raise Exception("Could not stop config server on %s" % self.host)

    def start(self):
        logging.info("Starting mongo configuration server on %s" % self.host)
        start_mongod = self.run("service mongod start")
        time.sleep(3)
        if not self.is_running():
            raise Exception("Could not start config server on %s" % self.host)

    def mongodump(self):
        """ Dump config database using mongodump. """
        logging.info("Dumping BackupConfigServer(%s)" % self.host)
        ret = self.run("mkdir -p %s" % self.backup_path)
        if ret != 0:
            raise Exception("Error dumping config database")
        ret = self.run("mongodump --port 27019 -d config -o %s" % self.backup_path)
        if ret != 0:
            raise Exception("Error dumping config database")
        
    def save_on_gcloud(self, backupDir='/backup/mongo*'):
        logging.info("uploading on gcloud %s" % self.host)
        ret = self.run("screen -d -m gsutil -m mv %s gs://bout-mongodb-backup/" % backupDir)
        if ret != 0:
            err = "Cannot upload succesfully %s at %s" % (backupDir, self.host)
            logging.error(err)    

class BackupHost(SSHAble):

    """ Physical server that we will be creating LVM snapshots on. """

    def __init__(self, host):
        logging.info("Initializing BackupHost(%s)" % host)
        self.host = host
        hostinfo = host.split(":");
        self.port = hostinfo[1];

    def mongodump_bout_db(self, backup_id, errors):
        logging.info("Taking a Dump on  %s on %s" % (backup_id, self.host))
        backup_path = "/backup/%s/%s" % (backup_id, self.host);
        ret = self.run("mkdir -p %s" % backup_path)
        if ret != 0:
            err = "Cannot dump %s at %s" % (backup_id, self.host)
            logging.error(err)
            errors.put(err)
        ret = self.run("mongodump --port %s -d bout_db -j 10 --gzip --excludeCollection user_order -o %s" % (self.port, backup_path))
        if ret != 0:
            err = "Cannot dump %s at %s" % (backup_id, self.host)
            logging.error(err)
            errors.put(err)
            
    def mongodump_bout_rollupdb(self, backup_id, errors):
        logging.info("Taking a Dump on  %s on %s" % (backup_id, self.host))
        backup_path = "/backup/%s/%s" % (backup_id, self.host);
        ret = self.run("mkdir -p %s" % backup_path)
        if ret != 0:
            err = "Cannot dump %s at %s" % (backup_id, self.host)
            logging.error(err)
            errors.put(err)
        ret = self.run("mongodump --port %s -d bout_rollupdb -j 10 --gzip -o %s" % (self.port, backup_path))
        if ret != 0:
            err = "Cannot dump %s at %s" % (backup_id, self.host)
            logging.error(err)
            errors.put(err)   
            
    def mongodump_bout_contactdb(self, backup_id, errors):
        logging.info("Taking a Dump on  %s on %s" % (backup_id, self.host))
        backup_path = "/backup/%s/%s" % (backup_id, self.host);
        ret = self.run("mkdir -p %s" % backup_path)
        if ret != 0:
            err = "Cannot dump %s at %s" % (backup_id, self.host)
            logging.error(err)
            errors.put(err)
        ret = self.run("mongodump --port %s -d bout_contactdb -j 10 --gzip -o %s" % (self.port, backup_path))
        if ret != 0:
            err = "Cannot dump %s at %s" % (backup_id, self.host)
            logging.error(err)
            errors.put(err)     
    def save_on_gcloud(self, backupDir='/backup/mongo*'):
        logging.info("uploading %s on gcloud from %s" % (backupDir, self.host))
        ret = self.run("screen -d -m gsutil -m mv %s gs://bout-mongodb-backup/" % backupDir)
        if ret != 0:
            err = "Cannot upload succesfully %s at %s" % (backupDir, self.host)
            logging.error(err)


class BackupCluster:

    """ Main class that does all the hard work """

    def __init__(self, mongos, config_basedir):
        """ Initialize all children objects, checking input parameters
            sanity and verifying connections to various servers/services
        """
        logging.info("Initializing BackupCluster")
        self.backup_id = self.generate_backup_id()
        logging.info("Backup ID is %s" % self.backup_id)
        
        self.mongos = BackupMongos(mongos)
        self.hosts = [BackupHost(host) for host in self.mongos.get_shards()]
        self.shards = [BackupShard(host) for host in self.mongos.get_shards()]
        self.config_server = BackupConfigServer(
                                                self.mongos.get_config_servers(),
                                                os.path.join(config_basedir,
                                                self.backup_id),
                                                )
        self.rollback_steps = []

    def generate_backup_id(self):
        """ Generate unique time-based backup ID that will be used both for
            configuration server backups and LVM snapshots of shard servers
        """
        ts = datetime.datetime.now()
        return  "mongo_%s" % ts.strftime('%Y%m%d-%H')
#        return ts.strftime('%Y%m%d-%H%M%S')

    def wait_for_locks(self):
        """ Loop until all shard locks are released.
            Give up after 30 minutes.
        """
        retries = 0
        while len(self.mongos.get_locks()) and retries < 360:
            logging.info("Waiting for locks to be released: %s" %
                         self.mongos.get_locks())
            time.sleep(5)
            retries += 1

        if len(self.mongos.get_locks()):
            raise Exception("Something is still locking the cluster,"
                            " aborting backup")

    def lock_shards(self):
        """ Lock all shards. As we would like to minimize the amount of time
            the cluster stays locked, we lock each shard in a separate thread.
            The queue is used to pass any errors back from the worker threads.
        """
        errors = Queue.Queue()
        threads = []
        for shard in self.shards:
            t = threading.Thread(target=shard.lock, args=(errors, ))
            threads.append(t)
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        if not errors.empty():
            # We don't really care for all errors, so just through the first one
            raise Exception(errors.get())

    def mongodump(self):
        """ Create LVM snapshots on the hosts. The cluster is supposed to be
            in a locked state at this point, so we use a separate thread for
            each server to create all the snapshots as fast as possible.
            The queue is used to pass any errors back from the worker threads.
        """
        errors = Queue.Queue()
        threads = []
        for host in self.hosts:
            t = threading.Thread(target=host.mongodump_bout_db,
                                 args=(self.backup_id, errors))
            threads.append(t)
            t = threading.Thread(target=host.mongodump_bout_rollupdb,
                                 args=(self.backup_id, errors))
            threads.append(t)
            t = threading.Thread(target=host.mongodump_bout_contactdb,
                                 args=(self.backup_id, errors))
            threads.append(t)
            
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        if not errors.empty():
            # We don't really care for all errors, so just through the first one
            raise Exception(errors.get())

    def unlock_shards(self):
        """ Unlock the shards. This should be pretty fast, so we don't play the
            threading game here, choosing simplicity over speed.
        """
        exceptions = []
        for shard in self.shards:
            try:
                shard.unlock()
            except Exception as e:
                # try to unlock as many shards as possible before throwing an
                # exception
                exceptions += str(e)
        if len(exceptions):
            raise Exception(", ".join(exceptions))
        
    def backup_on_cloud(self):
        """ Unlock the shards. This should be pretty fast, so we don't play the
            threading game here, choosing simplicity over speed.
        """
        errors = Queue.Queue()
        threads = []
        for host in self.hosts:
            t = threading.Thread(target=host.save_on_gcloud)
            threads.append(t)
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        if not errors.empty():
            # We don't really care for all errors, so just through the first one
            raise Exception(errors.get())

    def run_step(self, function, tries=1):
        """ Try executing a function, retrying up to `tries` times if it fails
            with an exception. If the function fails after all tries, roll back
            all the changes - basically, execute all steps from rollback_steps
            ignoring any exceptions. Hopefully, this should bring the cluster
            back into pre-backup state.
        """
        for i in range(tries):
            try:
                logging.debug("Running %s (try #%d)" % (function, i + 1))
                function()
                break
            except Exception as e:
                logging.info("Got an exception (%s) while running %s" %
                             (e, function))
                if (i == tries-1):
                    logging.info("Rolling back...")
                    for step in self.rollback_steps:
                        try:
                            step()
                        except (Exception, pymongo.errors.OperationFailure) as e:
                            logging.info("Got an exception (%s) while rolling"
                                         " back (step %s). Ignoring" %
                                         (e, step))
                    raise BackupAbortedException
                time.sleep(2)  # delay before re-trying

    def backup(self):
        """ This is basically a runlist of all steps required to backup a
            cluster. Before executing each step, a corresponding rollback
            function is pushed into the rollback_steps array to be able to
            get cluster back into production state in case something goes
            wrong.
        """
        global configReplica
        self.rollback_steps.insert(0, self.mongos.start_balancer)
        self.run_step(self.mongos.stop_balancer, 2)

        self.run_step(self.wait_for_locks)

        self.rollback_steps.insert(0, configReplica.unlock)
        self.run_step(configReplica.lock)

        self.run_step(self.config_server.mongodump, 3)

        self.rollback_steps.insert(0, self.unlock_shards)
        self.run_step(self.lock_shards)

        self.run_step(self.mongodump)

        self.rollback_steps.remove(self.unlock_shards)
        self.run_step(self.unlock_shards, 2)

        self.rollback_steps.remove(configReplica.unlock)
        self.run_step(configReplica.unlock, 2)

        self.rollback_steps.remove(self.mongos.start_balancer)
        self.run_step(self.mongos.start_balancer, 4)  # it usually starts on
            # the second try
        logging.info("Finished successfully")
        self.run_step(self.config_server.save_on_gcloud) # store config data on google cloud
        self.run_step(self.backup_on_cloud) # store shards data on google cloud
        logging.info("Uploaded successfully")
        
if __name__ == '__main__':
    mongos = '172.16.16.65:27017'
    backupdir = '/backup/'
    backup = BackupCluster(mongos, backupdir)
    backup.backup()
