package org.opendatakit.services.utilities;

import android.app.Fragment;
import android.content.Intent;

import com.google.zxing.integration.android.IntentIntegrator;

/**
 * Created by aditya on 3/10/2018.
 */

public final class FragmentIntentIntegrator extends IntentIntegrator {

    private final Fragment fragment;

    public FragmentIntentIntegrator(Fragment fragment) {
        super(fragment.getActivity());
        this.fragment = fragment;
    }

    @Override
    protected void startActivityForResult(Intent intent, int code) {
        fragment.startActivityForResult(intent, code);
    }
}