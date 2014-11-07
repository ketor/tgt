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
#include <pthread.h>

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
	
	int io_result;
	struct bs_rbd_aio_info *rbd_info;
};

struct bs_rbd_aio_info {
	struct list_head dev_list_entry;

	struct list_head cmd_wait_list;
	unsigned int nwaiting;
	unsigned int npending;
	unsigned int iodepth;

	int resubmit;

	struct scsi_lu *lu;

	int exit_flag;
	pthread_mutex_t waitlist_lock;
	pthread_cond_t  waitlist_cond;
	pthread_mutex_t callback_lock;
	pthread_cond_t  callback_cond;
	
	pthread_t submit_thread[AIO_MAX_IODEPTH];

	char *poolname;
	char *imagename;
	char *snapname;
	rados_t cluster;
	rados_ioctx_t ioctx;
	rbd_image_t rbd_image;
};

static struct list_head bs_rbd_aio_dev_list = LIST_HEAD_INIT(bs_rbd_aio_dev_list);

static inline struct bs_rbd_aio_info *BS_RBD_AIO_I(struct scsi_lu *lu)
{
	return (struct bs_rbd_aio_info *) ((char *)lu + sizeof(*lu));
}

static inline struct bs_rbd_aio_info *RBDP(struct scsi_lu *lu)
{
	return (struct bs_rbd_aio_info *) ((char *)lu + sizeof(*lu));
}

static void bs_rbd_finish_aiocb(rbd_completion_t comp, void *data)
{
	struct rbd_iocb *rbd_iocb = data;
	
	//rbd_iocb->io_result = rbd_aio_get_return_value(rbd_iocb->completion);
	rbd_aio_release(rbd_iocb->completion);
	
	struct scsi_cmd *cmd = (void *)(unsigned long)rbd_iocb->data;
	dprintf("cmd: %p\n", cmd);
	target_cmd_io_done(cmd, SAM_STAT_GOOD);
	
	//pthread_mutex_lock(&rbd_iocb->rbd_info->callback_lock);
	rbd_iocb->rbd_info->npending--;
	//pthread_cond_signal(&rbd_iocb->rbd_info->callback_cond);
	//pthread_mutex_unlock(&rbd_iocb->rbd_info->callback_lock);
	
	free(&rbd_iocb->completion);
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
	
	while(info->nwaiting == info->iodepth){
		printf("waiting [wait:%d][iodepth:%d]\n", info->nwaiting, info->iodepth);
	}
	
	//printf("nwaiting++ [wait:%d][iodepth:%d]\n", info->nwaiting, info->iodepth);
	pthread_mutex_lock(&info->waitlist_lock);
	list_add_tail(&cmd->bs_list, &info->cmd_wait_list);
	info->nwaiting++;
	set_cmd_async(cmd);
	pthread_cond_signal(&info->waitlist_cond);
	pthread_mutex_unlock(&info->waitlist_lock);
	//printf("nwaiting++ OK [wait:%d][iodepth:%d]\n", info->nwaiting, info->iodepth);
	
	return 0;
}

static void *submit_thread_fn(void *arg){
	struct bs_rbd_aio_info *info = (struct bs_rbd_aio_info*)arg;
	struct scsi_cmd *cmd;
	unsigned int scsi_op;
	struct rbd_iocb *rbd_iocb;
	
	while(info->exit_flag == 0){
		//checkout cmd from bs_list
		pthread_mutex_lock(&info->waitlist_lock);
		if(info->nwaiting == 0){
			//pthread_cond_wait(&info->waitlist_cond, &info->waitlist_lock);
			
			//if(info->nwaiting == 0){
				//printf("000cond from wait [%d]\n", info->nwaiting);
				pthread_mutex_unlock(&info->waitlist_lock);
				continue;
			//}
		}
		
		if (!list_empty(&info->cmd_wait_list)) {
			cmd = list_entry(info->cmd_wait_list.prev, struct scsi_cmd,
					bs_list);
			//printf("get cmd %p\n", cmd);
		}else{
			printf("11111no cmd continue\n");
			pthread_mutex_unlock(&info->waitlist_lock);
			continue;
		}
		
		list_del(&cmd->bs_list);
		info->nwaiting--;
		//if(info->nwaiting > 0) pthread_cond_signal(&info->waitlist_cond);
		pthread_mutex_unlock(&info->waitlist_lock);
		//checkout cmd from bs_list end
		
		//printf("start do npending++ now\n");
		
		//do submit pending++
		//pthread_mutex_lock(&info->callback_lock);
		while(info->iodepth == info->npending){
			//printf("pthread_cond_wait npending++ now\n");
		//	pthread_cond_wait(&info->callback_cond, &info->callback_lock);
			usleep(1);
		}
		info->npending++;
		//printf("npending++ OK\n");
		//pthread_mutex_unlock(&info->callback_lock);
		//do submit pending++ end
		
		//printf("start do real submit now\n");
		
		//do real submit
		rbd_iocb = malloc(sizeof(*rbd_iocb));
		scsi_op = (unsigned int)cmd->scb[0];
		rbd_iocb->data = cmd;
		rbd_iocb->rbd_aio_offset = cmd->offset;
		rbd_iocb->rbd_info = info;
		rbd_iocb->io_result = 0;
		
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
		
		int r = rbd_aio_create_completion(rbd_iocb, bs_rbd_finish_aiocb,
						&(rbd_iocb->completion));
		if (r < 0) {
			eprintf("rbd_aio_write failed.\n");
			printf("rbd_aio_write failed.\n");
			goto failed;
		}
		if (rbd_iocb->rbd_aio_opcode == IO_CMD_PWRITE) {
		  	r = rbd_aio_write(info->rbd_image, rbd_iocb->rbd_aio_offset,
		  			  rbd_iocb->rbd_aio_nbytes, rbd_iocb->rbd_aio_buf,
		  			  rbd_iocb->completion);
		  	if (r < 0) {
		  		eprintf("rbd_aio_write failed.\n");
		  		printf("rbd_aio_write failed.\n");
		  		goto failed;
		  	}
		
		} else if (rbd_iocb->rbd_aio_opcode == IO_CMD_PREAD) {
			r = rbd_aio_read(info->rbd_image, rbd_iocb->rbd_aio_offset,
					  rbd_iocb->rbd_aio_nbytes, rbd_iocb->rbd_aio_buf,
					  rbd_iocb->completion);
			
			if (r < 0) {
				eprintf("rbd_aio_read failed.\n");
				printf("rbd_aio_read failed.\n");
				goto failed;
			}
		}
		//printf("start do submit OK\n");
		continue;
		
		failed:
			continue;
		//do real submit end
		
		
	}
	return NULL;
}

static int bs_rbd_aio_open(struct scsi_lu *lu, char *path, int *fd, uint64_t *size)
{
	struct bs_rbd_aio_info *info = BS_RBD_AIO_I(lu);
	int ret;
	uint32_t blksize = 0;

	info->iodepth = AIO_MAX_IODEPTH;
	info->exit_flag = 0;
	
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

	pthread_mutex_init(&info->callback_lock, NULL);
	pthread_mutex_init(&info->waitlist_lock, NULL);
	pthread_cond_init(&info->callback_cond, NULL);
	pthread_cond_init(&info->waitlist_cond, NULL);
	
	int i = 0;
	for(i = 0; i < 2; i++)
	{
		ret = pthread_create(&info->submit_thread[i], NULL, submit_thread_fn, (void*)info);
		if (ret) {
			return  ret;
		}
	}

	eprintf("%s opened successfully for tgt:%d lun:%"PRId64 "\n",
		path, info->lu->tgt->tid, info->lu->lun);

	if (!lu->attrs.no_auto_lbppbe)
		update_lbppbe(lu, blksize);

	return 0;
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
fail:
	if (confname)
		free(confname);
	if (clientid)
		free(clientid);
	if (virsecretuuid)
		free(virsecretuuid);
	if (given_cephx_key)
		free(given_cephx_key);
	
	return TGTADM_SUCCESS;
}

static void bs_rbd_aio_exit(struct scsi_lu *lu)
{
	struct bs_rbd_aio_info *rbd = RBDP(lu);
	rbd->exit_flag = 1;
	
	int i = 0;
	for( i = 0 ;i < 2; i++ )
	{
		pthread_join(rbd->submit_thread[i], NULL);
	}
	
	pthread_cond_destroy(&rbd->callback_cond);
	pthread_cond_destroy(&rbd->waitlist_cond);
	pthread_mutex_destroy(&rbd->callback_lock);
	pthread_mutex_destroy(&rbd->waitlist_lock);
	
	rados_shutdown(rbd->cluster);
}

static struct backingstore_template rbd_aio_bst = {
	.bs_name            = "rbd_aio",
	.bs_datasize        = sizeof(struct bs_rbd_aio_info),
	.bs_init            = bs_rbd_aio_init,
	.bs_exit            = bs_rbd_aio_exit,
	.bs_open            = bs_rbd_aio_open,
	.bs_close           = bs_rbd_aio_close,
	.bs_cmd_submit  	= bs_rbd_aio_cmd_submit,
};

void register_bs_module(void)
{
	register_backingstore_template(&rbd_aio_bst);
}
