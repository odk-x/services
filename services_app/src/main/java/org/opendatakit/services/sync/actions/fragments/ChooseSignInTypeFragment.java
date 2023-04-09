package org.opendatakit.services.sync.actions.fragments;

import android.graphics.Paint;
import android.os.Bundle;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.appcompat.app.AlertDialog;
import androidx.lifecycle.Lifecycle;
import androidx.navigation.Navigation;

import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.TextView;
import android.widget.Toast;

import com.google.android.material.dialog.MaterialAlertDialogBuilder;
import com.google.android.material.dialog.MaterialDialogs;

import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.services.R;
import org.opendatakit.services.utilities.Constants;
import org.opendatakit.services.utilities.UserState;

import java.util.HashMap;
import java.util.Map;

public class ChooseSignInTypeFragment extends LoginFragment {

    private class OnButtonClick implements View.OnClickListener {
        @Override
        public void onClick(View v) {
            if (v.getId() == R.id.btnAnonymousSignInLogin)
                signInAsAnonymousUser();
            else if (v.getId() == R.id.btnUserSignInLogin)
                navController.navigate(R.id.setCredentialsFragment);
        }
    }

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        // Inflate the layout for this fragment
        return inflater.inflate(R.layout.fragment_choose_sign_in_type, container, false);
    }

    private Button btnAnonymous, btnAuthenticated;
    private TextView tvServerUrl, tvTitle;

    private String titleText, btnAnonymousText, btnAuthenticatedText;
    private boolean btnAnonymousEnable, btnAuthenticatedEnable;
    private boolean isAnonymousAllowed;

    @Override
    public void onViewCreated(@NonNull View view, @Nullable Bundle savedInstanceState) {
        super.onViewCreated(view, savedInstanceState);
        findViewsAndAttachListeners(view);
        setupViewModelAndNavController();
    }

    private void findViewsAndAttachListeners(View view) {
        tvServerUrl = view.findViewById(R.id.tvServerUrlLogin);
        tvTitle = view.findViewById(R.id.tvTitleLogin);

        btnAnonymous = view.findViewById(R.id.btnAnonymousSignInLogin);
        btnAuthenticated = view.findViewById(R.id.btnUserSignInLogin);

        OnButtonClick onButtonClick = new OnButtonClick();
        btnAnonymous.setOnClickListener(onButtonClick);
        btnAuthenticated.setOnClickListener(onButtonClick);

        titleText = getString(R.string.drawer_sign_in_button_text);
        btnAnonymousText = getString(R.string.anonymous_user);
        btnAuthenticatedText = getString(R.string.authenticated_user);
        btnAnonymousEnable = true;
        btnAuthenticatedEnable = true;
        isAnonymousAllowed = true;
        updateUserInterface();
    }

    @Override
    protected void setupViewModelAndNavController() {
        super.setupViewModelAndNavController();

        loginViewModel.getServerUrl().observe(getViewLifecycleOwner(), s -> {
            tvServerUrl.setText(s);
            tvServerUrl.setPaintFlags(tvServerUrl.getPaintFlags() | Paint.UNDERLINE_TEXT_FLAG);
        });

        loginViewModel.getFunctionType().observe(getViewLifecycleOwner(), s -> {
            switch (s) {
                case (Constants.LOGIN_TYPE_SIGN_IN): {
                    switch (loginViewModel.getUserState()) {
                        case LOGGED_OUT: {
                            inTypeSignInStateLoggedOut();
                            break;
                        }
                        case ANONYMOUS: {
                            inTypeSignInStateAnonymous();
                            break;
                        }
                        case AUTHENTICATED_USER: {
                            inTypeSignInStateAuthenticated();
                            break;
                        }
                    }
                    break;
                }
                case (Constants.LOGIN_TYPE_UPDATE_CREDENTIALS):
                    switch (loginViewModel.getUserState()) {
                        case LOGGED_OUT: {
                            inTypeUpdateCredentialsStateLoggedOut();
                            break;
                        }
                        case ANONYMOUS: {
                            inTypeUpdateCredentialsStateAnonymous();
                            break;
                        }
                        case AUTHENTICATED_USER: {
                            inTypeUpdateCredentialsStateAuthenticated();
                            break;
                        }
                    }
                    break;
                case (Constants.LOGIN_TYPE_SWITCH_SIGN_IN_TYPE):
                    switch (loginViewModel.getUserState()) {
                        case LOGGED_OUT: {
                            inTypeSwitchMethodStateLoggedOut();
                            break;
                        }
                        case ANONYMOUS: {
                            inTypeSwitchMethodStateAnonymous();
                            break;
                        }
                        case AUTHENTICATED_USER: {
                            inTypeSwitchMethodStateAuthenticated();
                            break;
                        }
                    }
                    break;
            }
            updateUserInterface();
        });

        loginViewModel.checkIsAnonymousAllowed().observe(getViewLifecycleOwner(), aBoolean -> {
            isAnonymousAllowed = aBoolean;
            perhapsEnableButtons();
        });
    }

    private void signInAsAnonymousUser() {
        getProps().setProperties(getAnonymousProperties());
        if (!loginViewModel.isAnonymousMethodUsed()) {
            promptToVerifyAnonymous();
        } else {
            requireActivity().finish();
        }
    }

    private void inTypeSignInStateLoggedOut() {
        updateViewProperties(getString(R.string.drawer_sign_in_button_text), getString(R.string.anonymous_user),
                getString(R.string.authenticated_user), true, true);
    }

    private void inTypeSwitchMethodStateLoggedOut() {
        showToast("User is Logged Out!");
        updateViewProperties(getString(R.string.switch_sign_in_type), getString(R.string.anonymous_user),
                getString(R.string.authenticated_user), false, false);
    }

    private void inTypeUpdateCredentialsStateLoggedOut() {
        showToast("User is Logged Out!");
        updateViewProperties(getString(R.string.drawer_item_update_credentials), getString(R.string.anonymous_user),
                getString(R.string.authenticated_user), false, false);
    }

    private void inTypeSignInStateAnonymous() {
        showToast("User is already Anonymous!");
        updateViewProperties(getString(R.string.drawer_sign_in_button_text), getString(R.string.anonymous_user),
                getString(R.string.sign_in_using_credentials), false, true);
    }

    private void inTypeSwitchMethodStateAnonymous() {
        navController.navigate(R.id.action_chooseSignInTypeFragment_to_setCredentialsFragment);
    }

    private void inTypeUpdateCredentialsStateAnonymous() {
        showToast("User is not signed in using Credentials");
        updateViewProperties(getString(R.string.drawer_item_update_credentials), getString(R.string.anonymous_user),
                getString(R.string.sign_in_using_credentials), false, true);
    }

    private void inTypeSignInStateAuthenticated() {
        showToast("User is signed in using Credentials");
        updateViewProperties(getString(R.string.drawer_sign_in_button_text), getString(R.string.sign_in_as_anonymous),
                getString(R.string.drawer_item_update_credentials), true, true);
    }

    private void inTypeSwitchMethodStateAuthenticated() {
        updateViewProperties(getString(R.string.switch_sign_in_type), getString(R.string.anonymous_user),
                getString(R.string.drawer_item_update_credentials), true, false);
    }

    private void inTypeUpdateCredentialsStateAuthenticated() {
        navController.navigate(R.id.action_chooseSignInTypeFragment_to_setCredentialsFragment);
    }

    private void updateViewProperties(String titleText, String btnAnonymousText, String btnAuthenticatedText, boolean btnAnonymousEnable, boolean btnAuthenticatedEnable) {
        this.titleText = titleText;
        this.btnAnonymousText = btnAnonymousText;
        this.btnAuthenticatedText = btnAuthenticatedText;
        this.btnAnonymousEnable = btnAnonymousEnable;
        this.btnAuthenticatedEnable = btnAuthenticatedEnable;
    }

    private void updateUserInterface() {
        tvTitle.setText(titleText);
        btnAnonymous.setText(btnAnonymousText);
        btnAuthenticated.setText(btnAuthenticatedText);
        perhapsEnableButtons();
    }

    public static Map<String, String> getAnonymousProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CommonToolProperties.KEY_AUTHENTICATION_TYPE, "none");
        properties.put(CommonToolProperties.KEY_CURRENT_USER_STATE, UserState.ANONYMOUS.name());
        properties.put(CommonToolProperties.KEY_USERNAME, "");
        properties.put(CommonToolProperties.KEY_IS_USER_AUTHENTICATED, null);
        properties.put(CommonToolProperties.KEY_LAST_SYNC_INFO, null);
        properties.put(CommonToolProperties.KEY_DEFAULT_GROUP, "");
        properties.put(CommonToolProperties.KEY_ROLES_LIST, "");
        properties.put(CommonToolProperties.KEY_USERS_LIST, "");
        return properties;
    }

    @Override
    void disableButtons() {
        if (getLifecycle().getCurrentState().isAtLeast(Lifecycle.State.CREATED)) {
            btnAnonymous.setEnabled(false);
            btnAuthenticated.setEnabled(false);
        }
    }

    @Override
    void perhapsEnableButtons() {
        if (getLifecycle().getCurrentState().isAtLeast(Lifecycle.State.CREATED)) {
            btnAuthenticated.setEnabled(btnAuthenticatedEnable);
            btnAnonymous.setEnabled(btnAnonymousEnable && isAnonymousAllowed);
        }
    }

    private void promptToVerifyAnonymous() {

        /**
         * New dialog styling
         * MaterialAlertDialogBuilder is standard for all ODK-X Apps
         * OdkAlertDialogStyle present in AndroidLibrary is used to style this dialog
         * @params change MaterialAlertDialogBuilder to AlertDialog.Builder in case of any error and remove R.style... param!
         */

        AlertDialog alertDialog = new MaterialAlertDialogBuilder(requireActivity(),R.style.OdkXAlertDialogStyle)
                .setTitle("Signed in Successfully")
                .setMessage("Would you like to verify the settings now?")
                .setPositiveButton("Yes", (dialog, which) -> verifyServerSettings())
                .setNegativeButton("No", (dialog, which) -> requireActivity().finish())
                .setCancelable(false)
                .create();
        alertDialog.setCanceledOnTouchOutside(false);
        alertDialog.show();
    }

    private void showToast(String message) {
        Toast.makeText(requireActivity(), message, Toast.LENGTH_LONG).show();
    }
}