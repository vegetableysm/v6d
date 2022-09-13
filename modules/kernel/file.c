/** Copyright 2020-2022 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
#include "vineyard_fs.h"
#include "vineyard_i.h"
#include "msg_mgr.h"

MODULE_LICENSE("GPL");
MODULE_AUTHOR("Vineyard authors");
MODULE_DESCRIPTION("Vineyard filesystem for Linux.");
MODULE_VERSION("0.01");

// tools
void translate_u64_to_char(uint64_t num, char *name)
{
	int i = 0, j = 0;
	int temp;
	char c;

	while (num) {
		temp = num % 10;
		num /= 10;
		name[i] = temp + '0';
		i++;
	}

	for (; j < i / 2; j++) {
		c = name[j];
		name[j] = name[i - j - 1];
		name[i - j - 1] = c;
	}
}

uint64_t translate_char_to_u64(const char *name)
{
	uint64_t ret = 0;
	int i = 0;

	while (name[i] != 0) {
		if (name[i] >= '0' && name[i] <= '9') {
			ret = ret * 10 + name[i] - '0';
			i++;
		} else {
			break;
		}
	}
	return ret;
}

// file operations
static int vineyard_fs_open(struct inode *inode, struct file *file)
{
	struct vineyard_inode_info *i_info;
	struct vineyard_request_msg msg;
	struct vineyard_result_msg rmsg;
	struct vineyard_private_data *data;
	int ret;
	printk(KERN_INFO PREFIX "fake %s\n", __func__);

	i_info = get_vineyard_inode_info(inode);
	printk(KERN_INFO PREFIX "open: %llu\n", i_info->obj_id);

	msg.opt = VINEYARD_OPEN;
	msg.param._fopt_param.obj_id = i_info->obj_id;
	msg.param._fopt_param.type = i_info->obj_type;
	ret = send_request_msg(&msg);
	if (ret)
		return ret;

	ret = receive_result_msg(&rmsg);
	if (ret)
		return ret;

	if (rmsg.ret._fopt_ret.ret == 0) {
		data = (struct vineyard_private_data *)kmalloc(
			sizeof(struct vineyard_private_data), GFP_KERNEL);
		data->data_pointer =
			vineyard_bulk_kernel_addr + rmsg.ret._fopt_ret.data_offset;
		data->data_size = rmsg.ret._fopt_ret.data_size;
		data->header_pointer = vineyard_bulk_kernel_addr + rmsg.ret._fopt_ret.header_offset;
		data->header_size = rmsg.ret._fopt_ret.header_size;
		data->type = rmsg.ret._fopt_ret.type;
		file->private_data = (void *)data;
		printk(KERN_INFO PREFIX "open:%llx %llu\n",
		       (uint64_t)data->data_pointer, data->data_size);
		return 0;
	}
	return -1;
}

static ssize_t vineyard_fs_read(struct file *file, char __user *user,
				size_t length, loff_t *start)
{
	struct vineyard_private_data *data;
	size_t size = 0;
	size_t file_size;
	int ret;
	size_t phase1_size;
	printk(KERN_INFO PREFIX "fake %s\n", __func__);

	data = (struct vineyard_private_data *)file->private_data;
	printk(KERN_INFO PREFIX "read:%llx %llu %lld\n",
	       (uint64_t)data->data_pointer, data->data_size, *start);
	if (data->type == BLOB) {
		if (*start >= data->data_size)
			return 0;

		size = length + *start > data->data_size ? length + *start - data->data_size :
							length;
		printk(KERN_INFO PREFIX "read size:%lu bulk addr:%px, read addr:%px\n",
			size, vineyard_bulk_kernel_addr, data->data_pointer);
		ret = copy_to_user(user, data->data_pointer + *start, size);
		if (ret)
			return ret;
	} else if (data->type == TENSOR) {
		file_size = data->data_size + data->header_size;
		printk(KERN_INFO PREFIX "file size:%ld start:%lld\n", file_size, *start);
		if (*start >= file_size) {
			return 0;
		} else if (*start >= data->header_size) {
			size = length + *start > file_size ? file_size - *start : length;
			printk(KERN_INFO PREFIX "read size:%lu\n", size);
			ret = copy_to_user(user, data->data_pointer, size);
			if (ret)
				return ret;
		} else {
			size = length + *start > file_size ? file_size - *start : length;
			printk(KERN_INFO PREFIX "read size:%lu\n", size);
			if (size + *start > data->header_size) {
				phase1_size = data->header_size - *start;
				ret = copy_to_user(user, data->header_pointer + *start, phase1_size);
				if (ret)
					return ret;
				ret = copy_to_user(user + phase1_size, data->data_pointer, size - phase1_size);
				if (ret)
					return ret;
			} else {
				ret = copy_to_user(user, data->header_pointer + *start, size);
				if (ret)
					return ret;
			}
		}
	}
	*start = *start + size;
	printk(KERN_INFO PREFIX "read finished\n");
	return size;
}

static ssize_t vineyard_fs_write(struct file *file, const char __user *user,
				 size_t length, loff_t *start)
{
	printk(KERN_INFO PREFIX "fake %s\n", __func__);
	return -1;
}

static int vineyard_fs_flush(struct file *file, fl_owner_t id)
{
	printk(KERN_INFO PREFIX "fake %s\n", __func__);
	return 0;
}

int vineyard_fs_release(struct inode *inode, struct file *file)
{
	printk(KERN_INFO PREFIX "fake %s\n", __func__);
	if (file->private_data)
		kfree(file->private_data);
	return 0;
}

int vineyard_setattr(struct user_namespace *mnt_userns, struct dentry *entry,
		     struct iattr *attr)
{
	printk(KERN_INFO PREFIX "fake %s\n", __func__);
	return -1;
}

int vineyard_getattr(struct user_namespace *mnt_userns, const struct path *path,
		     struct kstat *stat, u32 mask, unsigned int flags)
{
	struct inode *inode;

	printk(KERN_INFO PREFIX "fake %s\n", __func__);
	inode = d_inode(path->dentry);
	if (!mask) {
		stat->result_mask = 0;
		return 0;
	}

	if (stat)
		generic_fillattr(mnt_userns, inode, stat);
	return 0;
}

int vineyard_update_time(struct inode *inode, struct timespec64 *now, int flags)
{
	printk(KERN_INFO PREFIX "fake %s\n", __func__);
	return 0;
}

// vineyard file operations struct.
static const struct file_operations vineyard_file_operations = {
	.open = vineyard_fs_open,
	.read = vineyard_fs_read,
	.write = vineyard_fs_write,
	.flush = vineyard_fs_flush,
	.release = vineyard_fs_release,
	.llseek	= generic_file_llseek,
};

static const struct inode_operations vineyard_file_inode_operations = {
	.setattr = vineyard_setattr,
	.getattr = vineyard_getattr,
	.update_time = vineyard_update_time,
};

// vineyard file interfaces
void vineyard_fs_init_file_inode(struct inode *inode)
{
	printk(KERN_INFO PREFIX "%s\n", __func__);
	inode->i_fop = &vineyard_file_operations;
	inode->i_op = &vineyard_file_inode_operations;
}
