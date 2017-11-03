/**
 * @author alex
 */
package com.xyzdrivers.controllers;

import com.xyzdrivers.services.SQLService;
import com.xyzdrivers.services.MembersService;

import java.io.*;
import java.sql.*;
import java.util.*;
import javax.servlet.*;
import javax.servlet.http.*;

public class AdminController extends HttpServlet {

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
            /* create Connection to DB */
            Class.forName("com.mysql.jdbc.Driver");
            SQLService JDBC = new SQLService(DriverManager.getConnection("jdbc:derby://localhost:1527/xyzdrivers", "root", "root"));
            /* testing getMembersList */
            List<Object[]> members = MembersService.getMembers(JDBC);
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
