use crate::disk::{PAGE_SIZE, PageId, DiskManager};
use std::{rc::Rc, cell::RefCell, cell::Cell};
use std::collections::HashMap;
use std::io;

// page
// buffer pool manager
// buffer pool
// frame
// buffer

#[derive(Debug, thiserror::Error)]
pub enum Error {
  #[error(transparent)]
  Io(#[from] io::Error),
  #[error("no free buffer available in buffer pool")]
  NoFreeBuffer,
}

pub type Page = [u8; PAGE_SIZE as usize];

#[derive(Default, Clone, Copy)]
pub struct BufferId(usize);

pub struct BufferPoolManager {
  disk: DiskManager,
  pool: BufferPool,
  page_table:HashMap<PageId, BufferId>,
}

pub struct BufferPool {
  frames: Vec<Frame>,
// buffer with this next_victim_id will be judged whether it is a victim next time.
  next_victim_id: BufferId,
}

#[derive(Debug, Default)]
pub struct Frame {
  usage_count: u64,
  buffer: Rc<Buffer>,
}

#[derive(Debug)]
pub struct Buffer {
  pub page_id: PageId,
  pub page: RefCell<Page>,
  pub is_dirty: Cell<bool>,
}

impl Default for Buffer {
    fn default() -> Self {
        Self {
            page_id: Default::default(),
            page: RefCell::new([0u8; PAGE_SIZE as usize]),
            is_dirty: Cell::new(false),
        }
    }
}

impl BufferPool {
    pub fn new(pool_size: usize) -> Self {
        let mut frames = vec![];
        frames.resize_with(pool_size, Default::default);
        let next_victim_id = BufferId::default();
        Self {
            frames,
            next_victim_id,
        }
    }

    fn size(&self) -> usize {
        self.frames.len()
    }

    // Returns the buffer id to be deleted next time.
    // Rule:
    // 1. If this finds the buffer whose usage_count = 0, returns it as a victim immediately.
    // 2. If the checked buffer is NOT referenced at the time, decrement its usage_count.
    // 3. If the checked buffer is referenced at the time, skip this. If this happens #size times, returns None.
    fn evict(&mut self) -> Option<BufferId> {
        let pool_size = self.size();
        let mut num_consecutively_checked_buffers = 0;

         loop {
          let next_victim_id = self.next_victim_id.0;
          let frame = &mut self.frames[next_victim_id];
          if frame.usage_count == 0 {
            // break self.next_victim_id;
            return Some(self.next_victim_id);
          }

          if Rc::get_mut(&mut frame.buffer).is_some() {
            // this buffer is not referenced by any other. (Rc::get_mut returns some if not referenced)
            frame.usage_count -= 1;
            num_consecutively_checked_buffers = 0;
          } else {
            num_consecutively_checked_buffers += 1;
            if num_consecutively_checked_buffers >= pool_size {
              return None;
            }
          }
          self.next_victim_id = self.increment_id(self.next_victim_id)
        };
    }

    fn increment_id(&self, buffer_id: BufferId) -> BufferId {
        let id = (buffer_id.0 + 1) % self.size();
        BufferId(id)
    }
}

impl BufferPoolManager {
    pub fn new(disk: DiskManager, pool: BufferPool) -> Self {
        let page_table = HashMap::new();
        Self {
            disk,
            pool,
            page_table,
        }
    }

    fn fetch_page(&mut self, page_id: PageId) -> Result<Rc<Buffer>, Error> {
        if let Some(&buffer_id) = self.page_table.get(&page_id) {
            let frame = &mut self.pool.frames[buffer_id.0];
            frame.usage_count += 1;

            return Ok(frame.buffer.clone())
        }

        let evicted_buffer_id = match self.pool.evict() {
            Some(buffer_id) => buffer_id,
            None => return Err(Error::NoFreeBuffer),
        };

        let update_frame = &mut self.pool.frames[evicted_buffer_id.0];
        let evict_page_id = update_frame.buffer.page_id;

        let buffer = Rc::get_mut(&mut update_frame.buffer).unwrap();

        if buffer.is_dirty.get() {
            // evictされる前にdiskに書き込む
            self.disk.write_page_data(evict_page_id, buffer.page.get_mut())?;
        }

        buffer.page_id = page_id;
        buffer.is_dirty.set(false);

        self.disk.read_page_data(page_id, buffer.page.get_mut())?;
        update_frame.usage_count = 1;

        let page = update_frame.buffer.clone();

        self.page_table.remove(&evict_page_id);
        self.page_table.insert(page_id, evicted_buffer_id);

        Ok(page)
    }

    // Copied from https://github.com/KOBA789/relly/blob/3b1e656b7ae67ba2ddde2ba7d2748816b4792d1e/src/buffer.rs#L150-L172
    pub fn create_page(&mut self) -> Result<Rc<Buffer>, Error> {
        let buffer_id = self.pool.evict().ok_or(Error::NoFreeBuffer)?;
        let frame = &mut self.pool.frames[buffer_id.0];
        let evict_page_id = frame.buffer.page_id;
        let page_id = {
            let buffer = Rc::get_mut(&mut frame.buffer).unwrap();
            if buffer.is_dirty.get() {
                self.disk
                    .write_page_data(evict_page_id, buffer.page.get_mut())?;
            }
            let page_id = self.disk.allocate_page();
            *buffer = Buffer::default();
            buffer.page_id = page_id;
            buffer.is_dirty.set(true);
            frame.usage_count = 1;
            page_id
        };
        let page = Rc::clone(&frame.buffer);
        self.page_table.remove(&evict_page_id);
        self.page_table.insert(page_id, buffer_id);
        Ok(page)
    }
}

// Copied from https://github.com/KOBA789/relly/blob/3b1e656b7ae67ba2ddde2ba7d2748816b4792d1e/src/buffer.rs#L185-L234
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempfile;

    #[test]
    fn test() {
        let page_size = PAGE_SIZE as usize;

        let mut hello = Vec::with_capacity(page_size);
        hello.extend_from_slice(b"hello");
        hello.resize(page_size, 0);
        let mut world = Vec::with_capacity(page_size);
        world.extend_from_slice(b"world");
        world.resize(page_size, 0);

        let disk = DiskManager::new(tempfile().unwrap()).unwrap();
        let pool = BufferPool::new(1);
        let mut bufmgr = BufferPoolManager::new(disk, pool);
        let page1_id = {
            let buffer = bufmgr.create_page().unwrap();
            assert!(bufmgr.create_page().is_err());
            let mut page = buffer.page.borrow_mut();
            page.copy_from_slice(&hello);
            buffer.is_dirty.set(true);
            buffer.page_id
        };
        {
            let buffer = bufmgr.fetch_page(page1_id).unwrap();
            let page = buffer.page.borrow();
            assert_eq!(&hello, page.as_ref());
        }
        let page2_id = {
            let buffer = bufmgr.create_page().unwrap();
            let mut page = buffer.page.borrow_mut();
            page.copy_from_slice(&world);
            buffer.is_dirty.set(true);
            buffer.page_id
        };
        {
            let buffer = bufmgr.fetch_page(page1_id).unwrap();
            let page = buffer.page.borrow();
            assert_eq!(&hello, page.as_ref());
        }
        {
            let buffer = bufmgr.fetch_page(page2_id).unwrap();
            let page = buffer.page.borrow();
            assert_eq!(&world, page.as_ref());
        }
    }
}

