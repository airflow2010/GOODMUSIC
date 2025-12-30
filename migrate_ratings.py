import argparse
import os
import random
import sys

from google.cloud import firestore

from ingestion import init_firestore_db

COLLECTION_NAME = "musicvideos"
USERS_COLLECTION = "users"


def rating_key_for_user_id(user_id: str) -> str:
    import base64
    token = base64.urlsafe_b64encode(user_id.encode("utf-8")).decode("ascii").rstrip("=")
    return token or "user"


def ensure_admin_user(db, user_id: str, auth_provider: str) -> str:
    doc_ref = db.collection(USERS_COLLECTION).document(user_id)
    doc = doc_ref.get()
    existing = doc.to_dict() if doc.exists else {}
    rating_key = existing.get("rating_key") or rating_key_for_user_id(user_id)
    data = {
        "rating_key": rating_key,
        "role": "admin",
        "status": "active",
        "auth_provider": auth_provider,
    }
    if not doc.exists:
        data["created_at"] = firestore.SERVER_TIMESTAMP
    if auth_provider == "google":
        data["email"] = user_id
    doc_ref.set(data, merge=True)
    return rating_key


def build_legacy_rating(video_data: dict) -> dict:
    if not video_data or not video_data.get("date_rated"):
        return {}
    rating = {
        "rating_music": video_data.get("rating_music", 3),
        "rating_video": video_data.get("rating_video", 3),
        "favorite": bool(video_data.get("favorite", False)),
        "rejected": bool(video_data.get("rejected", False)),
        "rated_at": video_data.get("date_rated"),
        "updated_at": firestore.SERVER_TIMESTAMP,
    }
    if video_data.get("genre"):
        rating["genre_override"] = video_data.get("genre")
    return rating


def main():
    parser = argparse.ArgumentParser(description="Migrate legacy ratings into user-specific rating maps.")
    parser.add_argument("--project", help="GCP project id (optional if ADC is configured).")
    parser.add_argument("--admin-user", help="Admin user id/email to receive legacy ratings.")
    parser.add_argument("--auth-provider", choices=["basic", "google"], help="Auth provider for the admin user.")
    parser.add_argument("--remove-legacy", action="store_true", help="Remove legacy rating fields after migration.")
    parser.add_argument("--add-rand", action="store_true", help="Backfill rand for docs missing it.")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without writing.")
    args = parser.parse_args()

    admin_user = args.admin_user or os.environ.get("AUTH_GOOGLE") or os.environ.get("AUTH_USERNAME")
    if not admin_user:
        print("❌ Missing admin user. Provide --admin-user or set AUTH_GOOGLE/AUTH_USERNAME.", file=sys.stderr)
        sys.exit(1)

    auth_provider = args.auth_provider
    if not auth_provider:
        auth_provider = "google" if admin_user == os.environ.get("AUTH_GOOGLE") else "basic"

    db = init_firestore_db(args.project)
    if not db:
        print("❌ Failed to initialize Firestore.", file=sys.stderr)
        sys.exit(1)

    rating_key = ensure_admin_user(db, admin_user, auth_provider)

    migrated = 0
    skipped = 0
    updated_rand = 0
    removed_legacy = 0

    docs = db.collection(COLLECTION_NAME).stream()
    for doc in docs:
        data = doc.to_dict() or {}
        updates = {}

        if args.add_rand and "rand" not in data:
            updates["rand"] = random.random()

        ratings = data.get("ratings") or {}
        if rating_key in ratings:
            if updates:
                if args.dry_run:
                    print(f"[dry-run] update rand for {doc.id}")
                else:
                    doc.reference.update(updates)
                updated_rand += 1
            skipped += 1
            continue

        legacy_rating = build_legacy_rating(data)
        if legacy_rating:
            updates[f"ratings.{rating_key}"] = legacy_rating
            migrated += 1

            if args.remove_legacy:
                updates.update({
                    "rating_music": firestore.DELETE_FIELD,
                    "rating_video": firestore.DELETE_FIELD,
                    "favorite": firestore.DELETE_FIELD,
                    "rejected": firestore.DELETE_FIELD,
                    "date_rated": firestore.DELETE_FIELD,
                })
                removed_legacy += 1
        else:
            skipped += 1

        if updates:
            if args.dry_run:
                print(f"[dry-run] update {doc.id}: {list(updates.keys())}")
            else:
                doc.reference.update(updates)
                if "rand" in updates:
                    updated_rand += 1

    print(f"✅ Done. Migrated: {migrated}, Skipped: {skipped}, Rand updated: {updated_rand}, Legacy removed: {removed_legacy}")


if __name__ == "__main__":
    main()
