use anyhow::{Context, Ok, Result};
use std::{
    collections::HashMap,
    fs::File,
    io::{Cursor, Read, Seek, SeekFrom},
};

use crate::data_model::{
    btree::page::Page, db_header::Dbheader, schema_record::SchemaRecord, table::Table,
};
use crate::serialisation::deserialize::Deserialize;

/// Abstract fetching pages from disk
pub struct Pager<'a> {
    file: &'a mut File,
    pub db_header: Dbheader,
    pub root_page: Page,
    pub schema_table: Table<SchemaRecord>,
    // TODO: implement cache invalidation
    cache: HashMap<u32, (Page, Cursor<Vec<u8>>)>,
}

impl<'a> Pager<'a> {
    pub fn new(file: &'a mut File) -> Result<Self> {
        let db_header = Dbheader::deserialize(file);
        let root_page = Page::deserialize(file);
        let schema_table = Table::<SchemaRecord>::new(file, &root_page.cell_pointers);
        let cache = HashMap::new();
        Ok(Self {
            file,
            db_header,
            root_page,
            schema_table,
            cache,
        })
    }

    /// Read a page passing in 1-indexed page number
    /// Returns the Page struct and the byte array of the page data
    pub fn read_page(&mut self, page_number: u32) -> Result<(Page, Cursor<Vec<u8>>)> {
        let Some(page) = self.cache.get(&page_number) else {
            let page_location = (page_number - 1) as u64 * self.db_header.page_size as u64;

            self.file
                .seek(SeekFrom::Start(page_location))
                .context("couldn't find page in file")?;

            let mut page_buff: Vec<u8> = vec![0; self.db_header.page_size as usize];
            self.file.read(&mut page_buff)?;
            let mut reader = Cursor::new(page_buff);
            let page = (Page::deserialize(&mut reader), reader);
            self.cache.insert(page_number, page.clone());
            return Ok(page);
        };

        std::result::Result::Ok(page.clone())
    }
}
