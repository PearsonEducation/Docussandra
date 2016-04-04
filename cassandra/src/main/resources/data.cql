
begin batch
// Pearson eCollege
insert into namespaces (id, name) values(57493f29-4a81-4c18-8952-4cc9e088a296, 'blogging-api');
insert into namespaces_name (id, name) values(57493f29-4a81-4c18-8952-4cc9e088a296, 'blogging-api');
insert into namespaces (id, name) values(fa3fa86c-e71a-46e1-8631-5f636de26348, 'remarks-api');
insert into namespaces_name (id, name) values(fa3fa86c-e71a-46e1-8631-5f636de26348, 'remarks-api');

insert into collections (id, namespace, name) values(3dc40a46-34b1-47fc-9981-6501f29bdcc3, 'blogging-api', 'blogs');
insert into collections_name (id, namespace, name) values(3dc40a46-34b1-47fc-9981-6501f29bdcc3, 'blogging-api', 'blogs');

insert into collections (id, namespace, name) values(15c69ab4-298d-40b1-9b7d-3d3a998ff8da, 'blogging-api', 'posts');
insert into collections_name (id, namespace, name) values(15c69ab4-298d-40b1-9b7d-3d3a998ff8da, 'blogging-api', 'posts');

insert into collections (id, namespace, name) values(7aa85c0d-3371-4c86-b87a-5f651c08cff2, 'blogging-api', 'comments');
insert into collections_name (id, namespace, name) values(7aa85c0d-3371-4c86-b87a-5f651c08cff2, 'blogging-api', 'comments');

insert into collections (id, namespace, name) values(2c714213-bdf6-426f-9ea6-103a15aae0c4, 'remarks-api', 'remarks');
insert into collections_name (id, namespace, name) values(2c714213-bdf6-426f-9ea6-103a15aae0c4, 'remarks-api', 'remarks');

insert into collections (id, namespace, name) values(066cf525-4258-4291-a227-4d26ec7d35c0, 'remarks-api', 'replies');
insert into collections_name (id, namespace, name) values(066cf525-4258-4291-a227-4d26ec7d35c0, 'remarks-api', 'replies');

insert into entities (id, namespace, collection, object) values (b52eb45b-6f1a-470f-a27a-14161a624ea4, 'blogging-api', 'blogs', textAsBlob('{"name":"Some old blog","description":"This is a blog about nothing... much like a Jerry Seinfeld sorta thing.","createdAt":"2012-12-13","upatedAt":"2012-12-13"}'));
insert into entities_idx (id, namespace, collection, key, value) values (b52eb45b-6f1a-470f-a27a-14161a624ea4, 'blogging-api', 'blogs', textAsBlob('name'), textAsBlob('Some old blog'));
insert into entities_idx (id, namespace, collection, key, value) values (b52eb45b-6f1a-470f-a27a-14161a624ea4, 'blogging-api', 'blogs', textAsBlob('description'), textAsBlob('This is a blog about nothing... much like a Jerry Seinfeld sorta thing.'));
insert into entities_idx (id, namespace, collection, key, value) values (b52eb45b-6f1a-470f-a27a-14161a624ea4, 'blogging-api', 'blogs', textAsBlob('createdAt'), textAsBlob('2012-12-13'));
insert into entities_idx (id, namespace, collection, key, value) values (b52eb45b-6f1a-470f-a27a-14161a624ea4, 'blogging-api', 'blogs', textAsBlob('updatedAt'), textAsBlob('2012-12-13'));

insert into entities (id, namespace, collection, object) values (cddbd454-fae2-4660-8700-8570d05af24c, 'blogging-api', 'blogs', textAsBlob('{"name":"Another Blog","description":"There is just nothing here."}'));
insert into entities_idx (id, namespace, collection, key, value) values (cddbd454-fae2-4660-8700-8570d05af24c, 'blogging-api', 'blogs', textAsBlob('name'), textAsBlob('Another Blog'));
insert into entities_idx (id, namespace, collection, key, value) values (cddbd454-fae2-4660-8700-8570d05af24c, 'blogging-api', 'blogs', textAsBlob('description'), textAsBlob('There is just nothing here.'));


insert into namespaces (id, name) values(fc37e764-29d4-4a54-ba36-d1363aee2725, 'core-api');
insert into namespaces_name (id, name) values(fc37e764-29d4-4a54-ba36-d1363aee2725, 'core-api');

insert into collections (id, namespace, name) values(78b50520-99b7-42fa-9361-abccc1bf8378, 'core-api', 'users');
insert into collections_name (id, namespace, name) values(78b50520-99b7-42fa-9361-abccc1bf8378, 'core-api', 'users');

insert into collections (id, namespace, name) values(9f847466-79a6-41d2-b892-04ad6c7d97fe, 'core-api', 'applications');
insert into collections_name (id, namespace, name) values(9f847466-79a6-41d2-b892-04ad6c7d97fe, 'core-api', 'applications');

insert into collections (id, namespace, name) values(e99dfcf8-cc5b-4f26-a748-f172b67afc81, 'core-api', 'tokens');
insert into collections_name (id, namespace, name) values(e99dfcf8-cc5b-4f26-a748-f172b67afc81, 'core-api', 'tokens');

insert into collections (id, namespace, name) values(06ea358f-d5f0-48fe-ba45-e2755902ff35, 'core-api', 'organizations');
insert into collections_name (id, namespace, name) values(06ea358f-d5f0-48fe-ba45-e2755902ff35, 'core-api', 'organizations');

insert into collections (id, namespace, name) values(d23edd26-e948-41ef-91e7-3af61545c899, 'core-api', 'roles');
insert into collections_name (id, namespace, name) values(d23edd26-e948-41ef-91e7-3af61545c899, 'core-api', 'roles');

apply batch;