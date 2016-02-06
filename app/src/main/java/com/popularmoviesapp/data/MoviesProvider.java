/*
 * Copyright (C) 2014 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.popularmoviesapp.data;

import android.annotation.TargetApi;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;

public class MoviesProvider extends ContentProvider
{
    private static final UriMatcher sUriMatcher = buildUriMatcher();
    private MoviesDbHelper mOpenHelper;

    private static SQLiteQueryBuilder sMovieByParameterSettingQueryBuilder = new SQLiteQueryBuilder();

    static final int MOVIE_WITH_POPULARITY = 100;
    static final int MOVIE_WITH_RATING = 101;

    private static final String sRatingSettingSelection =
            MoviesContract.MovieEntry.TABLE_NAME + "." + MoviesContract.MovieEntry.COLUMN_USER_RATING + " = ? ";

    private static final String sPopularitySettingSelection =
            MoviesContract.MovieEntry.TABLE_NAME + "." + MoviesContract.MovieEntry.COLUMN_USER_POPULARITY + " = ? ";

    private Cursor getMovieByPopularity(Uri uri, String[] projection, String sortOrder)
    {
        String parameter = MoviesContract.MovieEntry.getParameterFromUri(uri);

        String selection = sPopularitySettingSelection;
        String[] selectionArgs = new String[]{parameter};

        return sMovieByParameterSettingQueryBuilder.query(mOpenHelper.getReadableDatabase(),
                projection,
                selection,
                selectionArgs,
                null,
                null,
                sortOrder
        );
    }

    private Cursor getMovieByRating( Uri uri, String[] projection, String sortOrder)
    {
        String parameter = MoviesContract.MovieEntry.getParameterFromUri(uri);

        String selection = sRatingSettingSelection;
        String[] selectionArgs = new String[]{parameter};

        return sMovieByParameterSettingQueryBuilder.query(mOpenHelper.getReadableDatabase(),
                projection,
                selection,
                selectionArgs,
                null,
                null,
                sortOrder
        );
    }
    
    static UriMatcher buildUriMatcher()
    {
        final UriMatcher matcher = new UriMatcher(UriMatcher.NO_MATCH);
        final String authority = MoviesContract.CONTENT_AUTHORITY;

        matcher.addURI(authority, MoviesContract.PATH_MOVIES , MOVIE_WITH_RATING);
        matcher.addURI(authority, MoviesContract.PATH_MOVIES , MOVIE_WITH_POPULARITY);
        return matcher;
    }
    
    @Override
    public boolean onCreate()
    {
        mOpenHelper = new MoviesDbHelper(getContext());
        return true;
    }

    @Override
    public String getType(Uri uri)
    {
        switch (sUriMatcher.match(uri))
        {
            case MOVIE_WITH_RATING:
                return MoviesContract.MovieEntry.CONTENT_TYPE;
            case MOVIE_WITH_POPULARITY:
                return MoviesContract.MovieEntry.CONTENT_TYPE;
            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder)
    {
        Cursor retCursor;

        switch (sUriMatcher.match(uri))
        {
            case MOVIE_WITH_RATING: retCursor = getMovieByRating(uri, projection, sortOrder);
                break;

            case MOVIE_WITH_POPULARITY: retCursor = getMovieByPopularity(uri, projection, sortOrder);
                break;

            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }
        retCursor.setNotificationUri(getContext().getContentResolver(), uri);
        return retCursor;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        Uri returnUri;

        long _id = db.insert(MoviesContract.MovieEntry.TABLE_NAME, null, values);
        if ( _id > 0 )
            returnUri = MoviesContract.MovieEntry.buildMovieUri(_id);
        else
            throw new android.database.SQLException("Failed to insert row into " + uri);

        getContext().getContentResolver().notifyChange(uri, null);
        return returnUri;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        int rowsDeleted;
        if ( null == selection ) selection = "1";
        rowsDeleted = db.delete(MoviesContract.MovieEntry.TABLE_NAME, selection, selectionArgs);

        if (rowsDeleted != 0) {
            getContext().getContentResolver().notifyChange(uri, null);
        }
        return rowsDeleted;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs)
    {
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        int rowsUpdated;

        rowsUpdated = db.update(MoviesContract.MovieEntry.TABLE_NAME, values, selection,selectionArgs);

        if (rowsUpdated != 0) {
            getContext().getContentResolver().notifyChange(uri, null);
        }
        return rowsUpdated;
    }

    @Override
    public int bulkInsert(Uri uri, ContentValues[] values)
    {
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();

        db.beginTransaction();
        int returnCount = 0;
        try
        {
            for (ContentValues value : values)
            {
                long _id = db.insert(MoviesContract.MovieEntry.TABLE_NAME, null, value);
                if (_id != -1)
                    returnCount++;
            }

            db.setTransactionSuccessful();
        }
        finally
        {
            db.endTransaction();
        }
        getContext().getContentResolver().notifyChange(uri, null);
        return returnCount;
    }

    @Override
    @TargetApi(11)
    public void shutdown() {
        mOpenHelper.close();
        super.shutdown();
    }
}