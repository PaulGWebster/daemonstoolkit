# daemontoolkit
A toolkit for those jobs you hope you never get

## misc
Random scribblings and rantings that have not had a module written for them yet, effectively snippets.

## src
The dzil and other sources for the various modules and such that make up the dt app

## bin
The finalized dt application as well as symlinks to various items in misc/ (long named)

This is to allow PATH to be exported to point directly to this directory.

Note that all binaries linked here from the misc/ directory are prefixed misc_ so as to not collide 
with any system installed utilities, if using a misc application beware that it will be removed the 
moment it is folded into the main dt application (or deleted)
