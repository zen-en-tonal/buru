-- Add up migration script here
create index image_tags_tag_name on image_tags(tag_name);
