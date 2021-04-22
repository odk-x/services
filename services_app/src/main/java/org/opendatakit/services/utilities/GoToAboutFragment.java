package org.opendatakit.services.utilities;

import androidx.fragment.app.Fragment;
import androidx.fragment.app.FragmentManager;
import androidx.fragment.app.FragmentTransaction;

import org.opendatakit.fragment.AboutMenuFragment;

public class GoToAboutFragment {
    public static void GotoAboutFragment(FragmentManager mgr, Integer id) {

        Fragment newFragment = mgr.findFragmentByTag(AboutMenuFragment.NAME);
        if (newFragment == null) {
            newFragment = new AboutMenuFragment();
        }
        if (!newFragment.isAdded()) {
            FragmentTransaction trans = mgr.beginTransaction();
            trans.replace(id, newFragment, AboutMenuFragment.NAME);
            trans.addToBackStack(AboutMenuFragment.NAME);
            trans.commit();
        }


    }
}
