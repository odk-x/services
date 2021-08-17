package org.opendatakit.services.sync.actions.fragments;

import android.graphics.Paint;
import android.os.Bundle;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.appcompat.app.AlertDialog;
import androidx.lifecycle.Lifecycle;

import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.TextView;

import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.services.R;
import org.opendatakit.services.utilities.Constants;
import org.opendatakit.services.utilities.UserState;

import java.util.HashMap;
import java.util.Map;

public class ChooseSignInTypeFragment extends LoginFragment {

    private class OnButtonClick implements View.OnClickListener{
        @Override
        public void onClick(View v) {
            if(v.getId()==R.id.btnAnonymousSignInLogin)
                signInAsAnonymousUser();
            else if(v.getId()==R.id.btnUserSignInLogin)
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

    @Override
    public void onViewCreated(@NonNull View view, @Nullable Bundle savedInstanceState) {
        super.onViewCreated(view, savedInstanceState);
        findViewsAndAttachListeners(view);
        setupViewModelAndNavController();
    }

    private void findViewsAndAttachListeners(View view){
        tvServerUrl=view.findViewById(R.id.tvServerUrlLogin);
        tvTitle=view.findViewById(R.id.tvTitleLogin);

        btnAnonymous=view.findViewById(R.id.btnAnonymousSignInLogin);
        btnAuthenticated=view.findViewById(R.id.btnUserSignInLogin);

        OnButtonClick onButtonClick=new OnButtonClick();
        btnAnonymous.setOnClickListener(onButtonClick);
        btnAuthenticated.setOnClickListener(onButtonClick);
    }

    @Override
    protected void setupViewModelAndNavController() {
        super.setupViewModelAndNavController();

        loginViewModel.getServerUrl().observe(getViewLifecycleOwner(), s -> {
            tvServerUrl.setText(s);
            tvServerUrl.setPaintFlags(tvServerUrl.getPaintFlags() | Paint.UNDERLINE_TEXT_FLAG);
        });

        loginViewModel.getFunctionType().observe(getViewLifecycleOwner(), s -> {
            switch (s){
                case (Constants.LOGIN_TYPE_SIGN_IN):{
                    tvTitle.setText(getString(R.string.drawer_sign_in_button_text));
                    break;
                }
                case (Constants.LOGIN_TYPE_UPDATE_CREDENTIALS):{
                    navController.navigate(R.id.action_chooseSignInTypeFragment_to_setCredentialsFragment);
                    break;
                }
                case (Constants.LOGIN_TYPE_SWITCH_SIGN_IN_TYPE): {
                    tvTitle.setText(R.string.switch_sign_in_type);
                    if (loginViewModel.getUserState() == UserState.ANONYMOUS) {
                        navController.navigate(R.id.action_chooseSignInTypeFragment_to_setCredentialsFragment);
                    } else {
                        btnAnonymous.setEnabled(true);
                        btnAuthenticated.setEnabled(false);
                    }
                    break;
                }
            }
        });

        loginViewModel.checkIsAnonymousAllowed().observe(getViewLifecycleOwner(), aBoolean -> btnAnonymous.setEnabled(aBoolean));
    }

    private void signInAsAnonymousUser(){
        updatePropertiesSingleton(getAnonymousProperties());
        if(!loginViewModel.isAnonymousMethodUsed()){
            promptToVerifyAnonymous();
        } else {
            requireActivity().finish();
        }
    }

    public Map<String,String> getAnonymousProperties(){
        Map<String,String> properties = new HashMap<>();
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
        if(getLifecycle().getCurrentState().isAtLeast(Lifecycle.State.CREATED)){
            btnAnonymous.setEnabled(false);
            btnAuthenticated.setEnabled(false);
        }
    }

    @Override
    void perhapsEnableButtons() {
        if(getLifecycle().getCurrentState().isAtLeast(Lifecycle.State.CREATED)){
            btnAuthenticated.setEnabled(true);
            btnAnonymous.setEnabled(loginViewModel.isAnonymousAllowed());
        }
    }

    private void promptToVerifyAnonymous(){
        AlertDialog alertDialog = new AlertDialog
                .Builder(requireActivity())
                .setTitle("Signed in Successfully")
                .setMessage("Would you like to verify the settings now?")
                .setPositiveButton("Yes", (dialog, which) -> verifyServerSettings())
                .setNegativeButton("No", (dialog, which) -> requireActivity().finish())
                .setCancelable(false)
                .create();
        alertDialog.setCanceledOnTouchOutside(false);
        alertDialog.show();
    }
}