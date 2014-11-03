/*
 * AIO backing store
 *
 * Copyright (C) 2006-2007 FUJITA Tomonori <tomof@acm.org>
 * Copyright (C) 2006-2007 Mike Christie <michaelc@cs.wisc.edu>
 * Copyright (C) 2011 Alexander Nezhinsky <alexandern@mellanox.com>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, version 2 of the
 * License.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */
#define _XOPEN_SOURCE 600

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <linux/fs.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/eventfd.h>

#include "list.h"
#include "util.h"
#include "tgtd.h"
#include "target.h"
#include "scsi.h"

#include "rados/librados.h"
#include "rbd/librbd.h"

#define IO_CMD_PREAD  0
#define IO_CMD_PWRITE 1

static void parse_imagepath(char *path, char **pool, char **image, char **snap)
{
	char *origp = strdup(path);
	char *p, *sep;

	p = origp;
	sep = strchr(p, '/');
	if (sep == NULL) {
		*pool = "rbd";
	} else {
		*sep = '\0';
		*pool = strdup(p);
		p = sep + 1;
	}
	/* p points to image[@snap] */
	sep = strchr(p, '@');
	if (sep == NULL) {
		*snap = "";
	} else {
		*snap = strdup(sep + 1);
		*sep = '\0';
	}
	/* p points to image\0 */
	*image = strdup(p);
	free(origp);
}

#define AIO_MAX_IODEPTH    128

struct rbd_iocb {
	rbd_completion_t completion;
	struct scsi_cmd *data;
	uint16_t rbd_aio_opcode;	/* see IOCB_CMD_ above */
	char *rbd_aio_buf;
	uint64_t rbd_aio_nbytes;
	int64_t	rbd_aio_offset;
	int rbd_aio_evt_fd;
	
	int io_complete;
	int result;
};

struct bs_rbd_aio_info {
	struct list_head dev_list_entry;

	struct list_head cmd_wait_list;
	unsigned int nwaiting;
	unsigned int npending;
	unsigned int iodepth;

	int resubmit;

	struct scsi_lu *lu;
	int evt_fd;
	
	char *poolname;
	char *imagename;
	char *snapname;
	rados_t cluster;
	rados_ioctx_t ioctx;
	rbd_image_t rbd_image;
};

static struct list_head bs_aio_dev_list = LIST_HEAD_INIT(bs_aio_dev_list);

static inline struct bs_rbd_aio_info *BS_RBD_AIO_I(struct scsi_lu *lu)
{
	return (struct bs_rbd_aio_info *) ((char *)lu + sizeof(*lu));
}

static inline struct bs_rbd_aio_info *RBDP(struct scsi_lu *lu)
{
	return (struct bs_rbd_aio_info *) ((char *)lu + sizeof(*lu));
}
/* bs_rbd_aio_info is allocated just after the bs_thread_info */
//#define RBDP(lu)	((struct bs_rbd_aio_info *) ((char *)lu +  sizeof(struct scsi_lu) + sizeof(struct bs_thread_info)))

static void bs_rbd_finish_aiocb(rbd_completion_t comp, void *data)
{
	struct rbd_iocb *rbd_iocb = data;
	
	rbd_iocb->io_complete = 1;
	rbd_iocb->result = rbd_aio_get_return_value(rbd_iocb->completion);
	uint64_t evts_complete = 1;
	write(rbd_iocb->rbd_aio_evt_fd, &evts_complete, sizeof(evts_complete));
	rbd_aio_release(rbd_iocb->completion);
	
	struct scsi_cmd *cmd = (void *)(unsigned long)rbd_iocb->data;
	dprintf("cmd: %p\n", cmd);
	target_cmd_io_done(cmd, SAM_STAT_GOOD);
	
	free(&rbd_iocb->completion);
}

static int bs_rbd_aio_submit_dev_batch(struct bs_rbd_aio_info *info)
{
	int nsubmit = 0, nsuccess = 0, i = 0;
	struct scsi_cmd *cmd, *next;

	nsubmit = info->iodepth - info->npending; /* max allowed to submit */
	if (nsubmit > info->nwaiting)
		nsubmit = info->nwaiting;

	dprintf("nsubmit:%d waiting:%d pending:%d, tgt:%d lun:%"PRId64 "\n",
		nsubmit, info->nwaiting, info->npending,
		info->lu->tgt->tid, info->lu->lun);

	if (!nsubmit)
		return 0;

	list_for_each_entry_safe(cmd, next, &info->cmd_wait_list, bs_list) {
		//bs_rbd_aio_iocb_prep(info, cmd);
		struct rbd_iocb *rbd_iocb = malloc(sizeof(*rbd_iocb));
		unsigned int scsi_op = (unsigned int)cmd->scb[0];
	
		rbd_iocb->data = cmd;
	
		switch (scsi_op) {
		case WRITE_6:
		case WRITE_10:
		case WRITE_12:
		case WRITE_16:
			rbd_iocb->rbd_aio_opcode = IO_CMD_PWRITE;
			rbd_iocb->rbd_aio_buf = scsi_get_out_buffer(cmd);
			rbd_iocb->rbd_aio_nbytes = scsi_get_out_length(cmd);
	
			dprintf("prep WR cmd:%p op:%x buf:0x%p sz:%lx\n",
				cmd, scsi_op, rbd_iocb->rbd_aio_buf, rbd_iocb->rbd_aio_nbytes);
			break;
	
		case READ_6:
		case READ_10:
		case READ_12:
		case READ_16:
			rbd_iocb->rbd_aio_opcode = IO_CMD_PREAD;
			rbd_iocb->rbd_aio_buf = scsi_get_in_buffer(cmd);
			rbd_iocb->rbd_aio_nbytes = scsi_get_in_length(cmd);
	
			dprintf("prep RD cmd:%p op:%x buf:0x%p sz:%lx\n",
				cmd, scsi_op, rbd_iocb->rbd_aio_buf, rbd_iocb->rbd_aio_nbytes);
			break;
	
		default:
			break;
		}
	
		rbd_iocb->rbd_aio_offset = cmd->offset;
		rbd_iocb->rbd_aio_evt_fd = info->evt_fd;
		rbd_iocb->io_complete = 0;
		rbd_iocb->result = 0;
		list_del(&cmd->bs_list);
		
		//rbd aio call
		int r = rbd_aio_create_completion(rbd_iocb, bs_rbd_finish_aiocb,
						&(rbd_iocb->completion));
		if (r < 0) {
			eprintf("rbd_aio_write failed.\n");
			goto failed;
		}
    	if (rbd_iocb->rbd_aio_opcode == IO_CMD_PWRITE) {
		  	r = rbd_aio_write(info->rbd_image, rbd_iocb->rbd_aio_offset,
		  			  rbd_iocb->rbd_aio_nbytes, rbd_iocb->rbd_aio_buf,
		  			  rbd_iocb->completion);
		  	if (r < 0) {
		  		eprintf("rbd_aio_write failed.\n");
		  		goto failed;
		  	}
    	
		} else if (rbd_iocb->rbd_aio_opcode == IO_CMD_PREAD) {
			r = rbd_aio_read(info->rbd_image, rbd_iocb->rbd_aio_offset,
					  rbd_iocb->rbd_aio_nbytes, rbd_iocb->rbd_aio_buf,
					  rbd_iocb->completion);
			
			if (r < 0) {
				eprintf("rbd_aio_read failed.\n");
				goto failed;
			}
		}
		
		nsuccess++;
		
		failed:
			continue;
		
		if (nsuccess == nsubmit)
			break;
	}
	
	if (unlikely(nsuccess < 0)) {
		if (nsuccess == -EAGAIN) {
			eprintf("delayed submit %d cmds to tgt:%d lun:%"PRId64 "\n",
				nsubmit, info->lu->tgt->tid, info->lu->lun);
			nsuccess = 0; /* leave the dev pending with all cmds */
		}
		else {
			eprintf("failed to submit %d cmds to tgt:%d lun:%"PRId64
				", err: %d\n",
				nsubmit, info->lu->tgt->tid,
				info->lu->lun, -nsuccess);
			return nsuccess;
		}
	}
	if (unlikely(nsuccess < nsubmit)) {
		for (i=nsubmit-1; i >= nsuccess; i--) {
			cmd = info->iocb_arr[i].data;
			list_add(&cmd->bs_list, &info->cmd_wait_list);
		}
	}

	info->npending += nsuccess;
	info->nwaiting -= nsuccess;
	/* if no cmds remain, remove the dev from the pending list */
	if (likely(!info->nwaiting))
			list_del(&info->dev_list_entry);

	dprintf("submitted %d of %d cmds to tgt:%d lun:%"PRId64
		", waiting:%d pending:%d\n",
		nsuccess, nsubmit, info->lu->tgt->tid, info->lu->lun,
		info->nwaiting, info->npending);
	return 0;
}

static int bs_rbd_aio_submit_all_devs(void)
{
	struct bs_rbd_aio_info *dev_info, *next_dev;
	int err;

	/* pass over all devices having some queued cmds and submit */
	list_for_each_entry_safe(dev_info, next_dev, &bs_aio_dev_list, dev_list_entry) {
		err = bs_rbd_aio_submit_dev_batch(dev_info);
		if (unlikely(err))
			return err;
	}
	return 0;
}

static int bs_rbd_aio_cmd_submit(struct scsi_cmd *cmd)
{
	struct scsi_lu *lu = cmd->dev;
	struct bs_rbd_aio_info *info = BS_RBD_AIO_I(lu);
	unsigned int scsi_op = (unsigned int)cmd->scb[0];

	switch (scsi_op) {
	case WRITE_6:
	case WRITE_10:
	case WRITE_12:
	case WRITE_16:
	case READ_6:
	case READ_10:
	case READ_12:
	case READ_16:
		break;

	case WRITE_SAME:
	case WRITE_SAME_16:
		eprintf("WRITE_SAME not yet supported for AIO backend.\n");
		return -1;

	case SYNCHRONIZE_CACHE:
	case SYNCHRONIZE_CACHE_16:
	default:
		dprintf("skipped cmd:%p op:%x\n", cmd, scsi_op);
		return 0;
	}

	list_add_tail(&cmd->bs_list, &info->cmd_wait_list);
	if (!info->nwaiting)
		list_add_tail(&info->dev_list_entry, &bs_aio_dev_list);
	info->nwaiting++;
	set_cmd_async(cmd);

	if (!cmd_not_last(cmd)) /* last cmd in batch */
		return bs_rbd_aio_submit_all_devs();

	if (info->nwaiting == info->iodepth - info->npending)
		return bs_rbd_aio_submit_dev_batch(info);

	return 0;
}

static void bs_rbd_aio_get_completions(int fd, int events, void *data)
{
	struct bs_rbd_aio_info *info = data;
	int ret;
	/* read from eventfd returns 8-byte int, fails with the error EINVAL
	   if the size of the supplied buffer is less than 8 bytes */
	uint64_t evts_complete;
	unsigned int ncomplete;

retry_read:
	ret = read(info->evt_fd, &evts_complete, sizeof(evts_complete));
	if (unlikely(ret < 0)) {
		eprintf("failed to read AIO completions, %m\n");
		if (errno == EAGAIN || errno == EINTR)
			goto retry_read;

		return;
	}
	ncomplete = (unsigned int) evts_complete;
	info->npending -= ncomplete;
	
	if (info->nwaiting) {
		dprintf("submit waiting cmds to tgt:%d lun:%"PRId64 "\n",
			info->lu->tgt->tid, info->lu->lun);
		bs_rbd_aio_submit_dev_batch(info);
	}
}

static int bs_rbd_aio_open(struct scsi_lu *lu, char *path, int *fd, uint64_t *size)
{
	struct bs_rbd_aio_info *info = BS_RBD_AIO_I(lu);
	int ret, afd;
	uint32_t blksize = 0;

	info->iodepth = AIO_MAX_IODEPTH;
	
	rbd_image_info_t inf;
	char *poolname;
	char *imagename;
	char *snapname;
	struct bs_rbd_aio_info *rbd = RBDP(lu);

	parse_imagepath(path, &poolname, &imagename, &snapname);
	
	rbd->poolname = poolname;
	rbd->imagename = imagename;
	rbd->snapname = snapname;
	eprintf("bs_rbd_open: pool: %s image: %s snap: %s\n",
		poolname, imagename, snapname);

	ret = rados_ioctx_create(rbd->cluster, poolname, &rbd->ioctx);
	if (ret < 0) {
		eprintf("bs_rbd_open: rados_ioctx_create: %d\n", ret);
		return -EIO;
	}

	ret = rbd_open(rbd->ioctx, imagename, &rbd->rbd_image, snapname);
	if (ret < 0) {
		eprintf("bs_rbd_open: rbd_open: %d\n", ret);
		return ret;
	}
	if (rbd_stat(rbd->rbd_image, &inf, sizeof(inf)) < 0) {
		eprintf("bs_rbd_open: rbd_stat: %d\n", ret);
		return ret;
	}
	*size = inf.size;
	blksize = inf.obj_size;

	eprintf("%s opened successfully for tgt:%d lun:%"PRId64 "\n",
		path, info->lu->tgt->tid, info->lu->lun);

	if (!lu->attrs.no_auto_lbppbe)
		update_lbppbe(lu, blksize);

	afd = eventfd(0, O_NONBLOCK);
	if (afd < 0) {
		eprintf("failed to create eventfd for tgt:%d lun:%"PRId64 ", %m\n",
			info->lu->tgt->tid, info->lu->lun);
		ret = afd;
		goto close_ctx;
	}
	dprintf("eventfd:%d for tgt:%d lun:%"PRId64 "\n",
		afd, info->lu->tgt->tid, info->lu->lun);

	ret = tgt_event_add(afd, EPOLLIN, bs_rbd_aio_get_completions, info);
	if (ret)
		goto close_eventfd;
	info->evt_fd = afd;

	return 0;

close_eventfd:
	close(afd);
close_ctx:
	return ret;
}

static void bs_rbd_aio_close(struct scsi_lu *lu)
{
	struct bs_rbd_aio_info *rbd = RBDP(lu);

	if (rbd->rbd_image) {
		rbd_close(rbd->rbd_image);
		rados_ioctx_destroy(rbd->ioctx);
		rbd->rbd_image = rbd->ioctx = NULL;
	}
}

// Slurp up and return a copy of everything to the next ';', and update p
static char *slurp_to_semi(char **p)
{
	char *end = index(*p, ';');
	char *ret;
	int len;

	if (end == NULL)
		end = *p + strlen(*p);
	len = end - *p;
	ret = malloc(len + 1);
	strncpy(ret, *p, len);
	ret[len] = '\0';
	*p = end;
	/* Jump past the semicolon, if we stopped at one */
	if (**p == ';')
		*p = end + 1;
	return ret;
}

static char *slurp_value(char **p)
{
	char *equal = index(*p, '=');
	if (equal) {
		*p = equal + 1;
		return slurp_to_semi(p);
	} else {
		// uh...no?
		return NULL;
	}
}

static int is_opt(const char *opt, char *p)
{
	int ret = 0;
	if ((strncmp(p, opt, strlen(opt)) == 0) &&
	    (p[strlen(opt)] == '=')) {
		ret = 1;
	}
	return ret;
}


static tgtadm_err bs_rbd_aio_init(struct scsi_lu *lu, char *bsopts)
{
	struct bs_rbd_aio_info *info = BS_RBD_AIO_I(lu);
	int i;

	memset(info, 0, sizeof(*info));
	INIT_LIST_HEAD(&info->dev_list_entry);
	INIT_LIST_HEAD(&info->cmd_wait_list);
	info->lu = lu;

	tgtadm_err ret = TGTADM_UNKNOWN_ERR;
	int rados_ret;
	struct bs_rbd_aio_info *rbd = RBDP(lu);
	char *confname = NULL;
	char *clientid = NULL;
	char *virsecretuuid = NULL;
	char *given_cephx_key = NULL;
	char disc_cephx_key[256];
	char *clustername = NULL;
	char clientid_full[128];
	char *ignore = NULL;

	dprintf("bs_rbd_init bsopts: \"%s\"\n", bsopts);

	// look for conf= or id= or cluster=

	while (bsopts && strlen(bsopts)) {
		if (is_opt("conf", bsopts))
			confname = slurp_value(&bsopts);
		else if (is_opt("id", bsopts))
			clientid = slurp_value(&bsopts);
		else if (is_opt("cluster", bsopts))
			clustername = slurp_value(&bsopts);
		else if (is_opt("virsecretuuid", bsopts))
			virsecretuuid = slurp_value(&bsopts);
		else if (is_opt("cephx_key", bsopts))
			given_cephx_key = slurp_value(&bsopts);
		else {
			ignore = slurp_to_semi(&bsopts);
			eprintf("bs_rbd: ignoring unknown option \"%s\"\n",
				ignore);
			free(ignore);
			break;
		}
	}

	if (clientid)
		eprintf("bs_rbd_init: clientid %s\n", clientid);
	if (confname)
		eprintf("bs_rbd_init: confname %s\n", confname);
	if (clustername)
		eprintf("bs_rbd_init: clustername %s\n", clustername);
	if (virsecretuuid)
		eprintf("bs_rbd_init: virsecretuuid %s\n", virsecretuuid);
	if (given_cephx_key)
		eprintf("bs_rbd_init: given_cephx_key %s\n", given_cephx_key);

	/* virsecretuuid && given_cephx_key are conflicting options. */
	if (virsecretuuid && given_cephx_key) {
		eprintf("Conflicting options virsecretuuid=[%s] cephx_key=[%s]",
			virsecretuuid, given_cephx_key);
		goto fail;
	}

	/* Get stored key from secret uuid. */
	if (virsecretuuid) {
		char libvir_uuid_file_path_buf[256] = "/etc/libvirt/secrets/";
		strcat(libvir_uuid_file_path_buf, virsecretuuid);
		strcat(libvir_uuid_file_path_buf, ".base64");

		FILE *fp;
		fp = fopen(libvir_uuid_file_path_buf , "r");
		if (fp == NULL) {
			eprintf("bs_rbd_init: Unable to read %s\n",
				libvir_uuid_file_path_buf);
			goto fail;
		}
		if (fgets(disc_cephx_key, 256, fp) == NULL) {
			eprintf("bs_rbd_init: Unable to read %s\n",
				libvir_uuid_file_path_buf);
			goto fail;
		}
		fclose(fp);
		strtok(disc_cephx_key, "\n");

		eprintf("bs_rbd_init: disc_cephx_key %s\n", disc_cephx_key);
	}

	eprintf("bs_rbd_init bsopts=%s\n", bsopts);
	/*
	 * clientid may be set by -i/--id. If clustername is set, then
	 * we use rados_create2, else rados_create
	 */
	if (clustername) {
		/* rados_create2 wants the full client name */
		if (clientid)
			snprintf(clientid_full, sizeof clientid_full,
				 "client.%s", clientid);
		else /* if not specified, default to client.admin */
			snprintf(clientid_full, sizeof clientid_full,
				 "client.admin");
		rados_ret = rados_create2(&rbd->cluster, clustername,
					  clientid_full, 0);
	} else {
		rados_ret = rados_create(&rbd->cluster, clientid);
	}
	if (rados_ret < 0) {
		eprintf("bs_rbd_init: rados_create: %d\n", rados_ret);
		return ret;
	}

	/*
	 * Read config from environment, then conf file(s) which may
	 * be set by conf=
	 */
	rados_ret = rados_conf_parse_env(rbd->cluster, NULL);
	if (rados_ret < 0) {
		eprintf("bs_rbd_init: rados_conf_parse_env: %d\n", rados_ret);
		goto fail;
	}
	rados_ret = rados_conf_read_file(rbd->cluster, confname);
	if (rados_ret < 0) {
		eprintf("bs_rbd_init: rados_conf_read_file: %d\n", rados_ret);
		goto fail;
	}

	/* Set given key */
	if (virsecretuuid) {
		if (rados_conf_set(rbd->cluster, "key", disc_cephx_key) < 0) {
			eprintf("bs_rbd_init: failed to set cephx_key: %s\n",
				disc_cephx_key);
			goto fail;
		}
	}
	if (given_cephx_key) {
		if (rados_conf_set(rbd->cluster, "key", given_cephx_key) < 0) {
			eprintf("bs_rbd_init: failed to set cephx_key: %s\n",
				given_cephx_key);
			goto fail;
		}
	}

	rados_ret = rados_connect(rbd->cluster);
	if (rados_ret < 0) {
		eprintf("bs_rbd_init: rados_connect: %d\n", rados_ret);
		goto fail;
	}
	//ret = bs_thread_open(info, bs_rbd_request, nr_iothreads);
fail:
	if (confname)
		free(confname);
	if (clientid)
		free(clientid);
	if (virsecretuuid)
		free(virsecretuuid);
	if (given_cephx_key)
		free(given_cephx_key);

	//return ret;
	
	return TGTADM_SUCCESS;
}

static void bs_rbd_aio_exit(struct scsi_lu *lu)
{
	struct bs_rbd_aio_info *rbd = RBDP(lu);

  close(rbd->evt_fd);
	rados_shutdown(rbd->cluster);
}

static struct backingstore_template aio_bst = {
	.bs_name		= "rbd_aio",
	.bs_datasize    	= sizeof(struct bs_rbd_aio_info),
	.bs_init		= bs_rbd_aio_init,
	.bs_exit		= bs_rbd_aio_exit,
	.bs_open		= bs_rbd_aio_open,
	.bs_close       	= bs_rbd_aio_close,
	.bs_cmd_submit  	= bs_rbd_aio_cmd_submit,
};

__attribute__((constructor)) static void register_bs_module(void)
{
	register_backingstore_template(&aio_bst);
}

