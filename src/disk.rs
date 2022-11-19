use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;
use std::io::Seek;

pub const PAGE_SIZE: u64 = 4096;

// #[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, FromBytes, AsBytes)]
// #[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct PageId(pub u64);

pub struct DiskManager{
    // ヒープファイルのファイルディスクリプタ
    heap_file: File,
    // 採番するページIDを決めるカウンタ
    next_page_id: u64,
}

impl DiskManager{
    pub fn new(data_file: File) -> io::Result<Self>  {
        let size = data_file.metadata()?.len();

        if size % PAGE_SIZE != 0 {
            return Err(io::Error::new(io::ErrorKind::Other, "unexpected file size"))
        }

        Ok(Self {
            heap_file: data_file,
            next_page_id: size / PAGE_SIZE,
        })
    }

    pub fn open(data_file_path: impl AsRef<Path>) -> io::Result<Self> {
        let heap_file = std::fs::OpenOptions::new().read(true).write(true).create(true).open(data_file_path)?;

        Self::new(heap_file)
    }

    pub fn allocate_page(&mut self) -> PageId {
        let page_id = self.next_page_id;
        self.next_page_id += 1;

        PageId(page_id)
    }

    pub fn read_page_data(&mut self, page_id: PageId, data: &mut [u8]) -> io::Result<()> {
        let offset = page_id.0 * PAGE_SIZE;

        self.heap_file.seek(std::io::SeekFrom::Start(offset))?;
        self.heap_file.read_exact(data)?;

        Ok(())
    }

    pub fn write_page_data(&mut self, page_id: PageId, data: &[u8]) -> io::Result<()> {
        let offset = page_id.0 * PAGE_SIZE;

        self.heap_file.seek(std::io::SeekFrom::Start(offset))?;
        self.heap_file.write_all(data)?;

        Ok(())
    }
}

// Copied from https://github.com/KOBA789/relly/blob/3b1e656b7ae67ba2ddde2ba7d2748816b4792d1e/src/disk.rs#L96-L123
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test() {
        let page_size = PAGE_SIZE as usize;

        let (data_file, data_file_path) = NamedTempFile::new().unwrap().into_parts();
        let mut disk = DiskManager::new(data_file).unwrap();
        let mut hello = Vec::with_capacity(page_size);
        hello.extend_from_slice(b"hello");
        hello.resize(page_size, 0);
        let hello_page_id = disk.allocate_page();
        disk.write_page_data(hello_page_id, &hello).unwrap();
        let mut world = Vec::with_capacity(page_size);
        world.extend_from_slice(b"world");
        world.resize(page_size, 0);
        let world_page_id = disk.allocate_page();
        disk.write_page_data(world_page_id, &world).unwrap();
        drop(disk);
        let mut disk2 = DiskManager::open(&data_file_path).unwrap();
        let mut buf = vec![0; page_size];
        disk2.read_page_data(hello_page_id, &mut buf).unwrap();
        assert_eq!(hello, buf);
        disk2.read_page_data(world_page_id, &mut buf).unwrap();
        assert_eq!(world, buf);
    }
}
