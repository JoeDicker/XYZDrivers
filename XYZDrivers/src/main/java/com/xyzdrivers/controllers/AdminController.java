/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.xyzdrivers.controllers;

import com.xyzdrivers.models.*;

import java.io.*;
import java.sql.*;
import java.util.*;
import javax.servlet.*;
import javax.servlet.http.*;

/**
 *
 * @author arthur
 */
public class AdminController extends HttpServlet {

    public List<Object[]> getMembersList(jdbcDriver JDBC)
    {
        List<Object[]> members = null;
        
        try {
            members = JDBC.retrieve("MEMBERS");
        } catch (SQLException ex) {
            System.err.println(ex);
        }
        
        return members;
    }
    
    /**
     * Processes requests for both HTTP <code>GET</code> and <code>POST</code>
     * methods.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException
    {
        /* ----- DEBUGGING-start ----- */
        try {
            jdbcDriver JDBC = null;
            JDBC = new jdbcDriver("jdbc:derby://localhost:1527/xyzdrivers", "root", "root");
            List<Object[]> members = getMembersList(JDBC);
            System.out.println(members.get(0)[2]);
        } catch (SQLException | ClassNotFoundException ex) {
            System.out.println("EXCEPTION: ");
            System.out.println(ex);
        }
        /* ----- DEBUGGING-end ----- */
    }

    // <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">
    /**
     * Handles the HTTP <code>GET</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /**
     * Handles the HTTP <code>POST</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /**
     * Returns a short description of the servlet.
     *
     * @return a String containing servlet description
     */
    @Override
    public String getServletInfo() {
        return "Short description";
    }// </editor-fold>
}
