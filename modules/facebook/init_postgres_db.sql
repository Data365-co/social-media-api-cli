CREATE SCHEMA IF NOT EXISTS anton;

CREATE TABLE IF NOT EXISTS anton.facebook_posts
(
    id bigint NOT NULL,
    created_time timestamp without time zone,
    timestamp integer,
    post_type text,

    text text,
    text_lang text,
    text_tagged_users bigint[],
    text_tags text[],

    attached_link text,
    attached_link_description text,
    attached_video_url text,
    attached_video_preview_url text,
    attached_image_url text,
    attached_image_content text,
    attached_medias_id bigint[],
    attached_medias_preview_url text[],
    attached_medias_preview_content text[],
    attached_post_id bigint,

    reactions_total_count integer,
    reactions_like_count integer,
    reactions_love_count integer,
    reactions_haha_count integer,
    reactions_wow_count integer,
    reactions_sad_count integer,
    reactions_angry_count integer,
    reactions_support_count integer,
    comments_count integer,
    shares_count integer,

    owner_id bigint,
    group_id bigint,

    CONSTRAINT facebook_posts_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS anton.facebook_comments
(
    id bigint NOT NULL,
    parent_id bigint NOT NULL,
    created_time timestamp without time zone,
    timestamp integer,

    text text,
    text_lang text,
    text_tagged_users bigint[],
    text_tags text[],

    reactions_total_count integer,
    reactions_like_count integer,
    reactions_love_count integer,
    reactions_haha_count integer,
    reactions_wow_count integer,
    reactions_sad_count integer,
    reactions_angry_count integer,
    reactions_support_count integer,
    comments_count integer,

    owner_id bigint,
    owner_username text,
    owner_full_name text,

    CONSTRAINT facebook_comments_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS anton.facebook_profiles
(
    id bigint NOT NULL,
    username text,
    full_name text,
    profile_type text,

    profile_photo_url text,
    profile_photo_thumb_big_url text,
    profile_photo_thumb_medium_url text,
    profile_photo_thumb_small_url text,

    biography text,
    workplace text[],
    education text[],
    current_city text[],
    hometown text[],
    categories text[],

    address text,
    latitude float,
    longitude float,
    phone text,
    external_url text,

    age_approx integer,
    age_group text,
    gender text,
    langs text[],

    likes_count integer,
    friends_count integer,
    followers_count integer,
    members_count integer,

    last_post_created_time timestamp without time zone,

    CONSTRAINT facebook_profiles_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS anton.facebook_searches_for_posts
(
    id text,
    request text,
    location_id bigint,
    author_id bigint,
    from_date timestamp without time zone,
    to_date timestamp without time zone,
    search_type text,

    CONSTRAINT facebook_searches_for_posts_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS anton.facebook_connections
(
    id bigint NOT NULL,
    parent_id text,
    collection text,

    CONSTRAINT facebook_connections_pkey PRIMARY KEY (id, parent_id, collection)
);
