# Fast Messenger Pro v14 — Realtime Database edition

This build uses the new Firebase project `fast-massage-3ac80`.

The old Firestore client/database has been removed from the app. Google Authentication remains enabled, while users, friends, friend requests, groups, messages, presence/typing and call signalling are stored in Firebase Realtime Database.

## Firebase configuration

- Project ID: `fast-massage-3ac80`
- Auth domain: `fast-massage-3ac80.firebaseapp.com`
- Realtime Database: `https://fast-massage-3ac80-default-rtdb.firebaseio.com`

## Required Firebase action

Open Firebase Console → Realtime Database → Rules and publish the contents of `database.rules.json`.

The ZIP cannot publish Firebase rules automatically.

## Call note

Audio/video calling uses Agora Web SDK. The Firebase database handles call invitations/status/signalling metadata. If Agora App Certificate/token authentication is enabled, an Agora token service is also required.


## v20 fixes
- Single-source realtime message timeline; sender/receiver listeners no longer race.
- Stable message timestamp across reaction/read-state updates.
- Reaction updates preserve chronological order and scroll position.
- Stronger unread chat card styling, including dark mode.


Update v22: reaction overlay overflow visibility and removable attachment queue controls.
