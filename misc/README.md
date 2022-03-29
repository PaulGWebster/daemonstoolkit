# NOTES

## Clearing databases in postgres

Fast and complete:

 ```DROP SCHEMA public CASCADE; CREATE SCHEMA public; GRANT USAGE on schema public to public; GRANT CREATE on schema public to public; DROP SCHEMA public CASCADE; CREATE SCHEMA public; GRANT USAGE on schema public to public; GRANT CREATE on schema public to public;```
