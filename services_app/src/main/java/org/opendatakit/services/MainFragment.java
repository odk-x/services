package org.opendatakit.services;

import android.content.Intent;
import android.graphics.Paint;
import android.os.Bundle;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.fragment.app.Fragment;
import androidx.lifecycle.ViewModelProvider;

import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.TextView;

import org.opendatakit.consts.IntentConsts;
import org.opendatakit.services.sync.actions.activities.LoginActivity;
import org.opendatakit.services.sync.actions.viewModels.AbsSyncViewModel;
import org.opendatakit.services.utilities.DateTimeUtil;
import org.opendatakit.services.utilities.UserState;

public class MainFragment extends Fragment {

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
        // Inflate the layout for this fragment
        return inflater.inflate(R.layout.fragment_main, container, false);
    }

    private TextView tvServerUrl, tvUserState, tvUsernameLabel, tvUsername, tvLastSyncTimeLabel, tvLastSyncTime;
    private Button btnSignIn;

    private AbsSyncViewModel absSyncViewModel;

    @Override
    public void onViewCreated(@NonNull View view, @Nullable Bundle savedInstanceState) {
        super.onViewCreated(view, savedInstanceState);
        findViewsAndAttachListeners(view);
        setupViewModelAndNavController();
    }

    private void findViewsAndAttachListeners(View view) {
        tvServerUrl = view.findViewById(R.id.tvServerUrlMain);
        tvUserState = view.findViewById(R.id.tvUserStateMain);
        tvUsernameLabel = view.findViewById(R.id.tvUsernameLabelMain);
        tvUsername = view.findViewById(R.id.tvUsernameMain);
        tvLastSyncTimeLabel = view.findViewById(R.id.tvLastSyncTimeLabelMain);
        tvLastSyncTime = view.findViewById(R.id.tvLastSyncTimeMain);

        btnSignIn = view.findViewById(R.id.btnSignInMain);

        btnSignIn.setOnClickListener(v -> onSignInButtonClicked());
    }

    private void setupViewModelAndNavController() {
        absSyncViewModel = new ViewModelProvider(requireActivity()).get(AbsSyncViewModel.class);

        absSyncViewModel.getServerUrl().observe(getViewLifecycleOwner(), s -> {
            tvServerUrl.setText(s);
            tvServerUrl.setPaintFlags(tvServerUrl.getPaintFlags() | Paint.UNDERLINE_TEXT_FLAG);
        });

        absSyncViewModel.getCurrentUserState().observe(getViewLifecycleOwner(), userState -> {
            if (userState == UserState.LOGGED_OUT) {
                inLoggedOutState();
            } else if (userState == UserState.ANONYMOUS) {
                inAnonymousState();
            } else {
                inAuthenticatedState();
            }
        });

        absSyncViewModel.checkIsLastSyncTimeAvailable().observe(getViewLifecycleOwner(), aBoolean -> {
            if (!aBoolean)
                tvLastSyncTime.setText(getString(R.string.last_sync_not_available));
        });

        absSyncViewModel.getLastSyncTime().observe(getViewLifecycleOwner(), aLong -> tvLastSyncTime.setText(DateTimeUtil.getDisplayDate(aLong)));

        absSyncViewModel.getUsername().observe(getViewLifecycleOwner(), s -> tvUsername.setText(s));
    }

    /**
     * Actions on Clicking on the Sign-In Button
     */
    private void onSignInButtonClicked() {
        Intent signInIntent = new Intent(requireActivity(), LoginActivity.class);
        signInIntent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, absSyncViewModel.getAppName());
        startActivity(signInIntent);
    }

    /**
     * Actions in the Logged-Out User State
     */
    private void inLoggedOutState() {
        handleViewVisibility(View.VISIBLE, View.GONE, View.GONE);
        tvUserState.setText(R.string.logged_out);
    }

    /**
     * Actions in the Anonymous User State
     */
    private void inAnonymousState() {
        handleViewVisibility(View.GONE, View.GONE, View.VISIBLE);
        tvUserState.setText(R.string.anonymous_user);
    }

    /**
     * Actions in the Authenticated User State
     */
    private void inAuthenticatedState() {
        handleViewVisibility(View.GONE, View.VISIBLE, View.VISIBLE);
        tvUserState.setText(R.string.authenticated_user);
    }

    /**
     * Sets the Visibility of Different Views on the Main Screen
     *
     * @param btnSignInVisible    : The Visibility of the Sign-In Button
     * @param usernameVisible     : The Visibility of the Username
     * @param lastSyncTimeVisible : The Visibility of the Last Sync Time
     */
    private void handleViewVisibility(int btnSignInVisible, int usernameVisible, int lastSyncTimeVisible) {
        btnSignIn.setVisibility(btnSignInVisible);
        tvUsernameLabel.setVisibility(usernameVisible);
        tvUsername.setVisibility(usernameVisible);
        tvLastSyncTimeLabel.setVisibility(lastSyncTimeVisible);
        tvLastSyncTime.setVisibility(lastSyncTimeVisible);
    }

}